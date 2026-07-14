"""Phase 3 medallion pipeline DAG for the Olist project."""

from __future__ import annotations

import json
import logging
import os
import socket
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Tuple
from urllib.parse import urljoin, urlparse

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pyhive import hive

from processing.config import HDFS_URI, PROJECT_ROOT, RAW_DATA_DIR
from processing.spark_session import create_spark_session


logger = logging.getLogger(__name__)

DAG_ID = "olist_medallion_pipeline"
LONG_TASK_TIMEOUT = timedelta(hours=2)
SHORT_TASK_TIMEOUT = timedelta(minutes=20)

AIRFLOW_SPARK_MASTER_URL = os.getenv("AIRFLOW_SPARK_MASTER_URL", "spark://spark-master:7077")
AIRFLOW_SPARK_THRIFTSERVER_HOST = os.getenv("AIRFLOW_SPARK_THRIFTSERVER_HOST", "spark-thriftserver")
AIRFLOW_SPARK_THRIFTSERVER_PORT = int(os.getenv("AIRFLOW_SPARK_THRIFTSERVER_PORT", "10000"))
AIRFLOW_HDFS_URI = os.getenv("AIRFLOW_HDFS_URI", HDFS_URI)
AIRFLOW_DBT_TARGET = os.getenv("AIRFLOW_DBT_TARGET", "local")
AIRFLOW_DBT_DATABASE = os.getenv("AIRFLOW_DBT_DATABASE", "default")
AIRFLOW_DBT_SCHEMA = os.getenv("AIRFLOW_DBT_SCHEMA", "olist_analytics")
AIRFLOW_DBT_BRONZE_SCHEMA = os.getenv("AIRFLOW_DBT_BRONZE_SCHEMA", "olist_bronze")
AIRFLOW_DBT_SILVER_SCHEMA = os.getenv("AIRFLOW_DBT_SILVER_SCHEMA", "silver")
AIRFLOW_DBT_GOLD_SCHEMA = os.getenv("AIRFLOW_DBT_GOLD_SCHEMA", "gold")
AIRFLOW_DBT_THREADS = int(os.getenv("AIRFLOW_DBT_THREADS", "4"))
AIRFLOW_DBT_USER = os.getenv("AIRFLOW_DBT_USER", "airflow")
AIRFLOW_SUPERSET_URL = os.getenv("AIRFLOW_SUPERSET_URL", "http://superset:8088")
AIRFLOW_SUPERSET_HEALTH_ENDPOINT = os.getenv("AIRFLOW_SUPERSET_HEALTH_ENDPOINT", "/health")
AIRFLOW_SUPERSET_REFRESH_ENDPOINT = os.getenv("AIRFLOW_SUPERSET_REFRESH_ENDPOINT", "/health")
AIRFLOW_SUPERSET_USERNAME = os.getenv("AIRFLOW_SUPERSET_USERNAME", "")
AIRFLOW_SUPERSET_PASSWORD = os.getenv("AIRFLOW_SUPERSET_PASSWORD", "")
AIRFLOW_SUPERSET_ACCESS_TOKEN = os.getenv("AIRFLOW_SUPERSET_ACCESS_TOKEN", "")

DBT_PROJECT_DIR = Path("/opt/airflow/dbt")
DBT_PROFILES_DIR = Path("/opt/airflow/dbt")
REPORTS_RUNS_DIR = PROJECT_ROOT / "reports" / "phase3" / "runs"


def _task_failure_callback(context: Dict[str, Any]) -> None:
    exception = context.get("exception")
    logger.error(
        "Task failure captured for dag_id=%s task_id=%s run_id=%s",
        context.get("dag").dag_id if context.get("dag") else None,
        context.get("task_instance").task_id if context.get("task_instance") else None,
        context.get("run_id"),
    )
    if exception is not None:
        try:
            raise exception
        except Exception:
            logger.exception("Task exception details")


def _pipeline_run_id(context: Mapping[str, Any]) -> str:
    dag_run = context.get("dag_run")
    if dag_run is not None and getattr(dag_run, "run_id", None):
        return str(dag_run.run_id)
    return str(context.get("run_id") or "unknown-run")


def _load_csv_names() -> list[str]:
    return [csv_file.stem for csv_file in sorted(RAW_DATA_DIR.glob("*.csv"))]


def _parse_host_port(uri: str, default_port: int) -> Tuple[str, int]:
    parsed = urlparse(uri if "://" in uri else f"//{uri}")
    host = parsed.hostname or uri
    port = parsed.port or default_port
    return host, port


def _check_tcp_service(service_name: str, host: str, port: int, timeout_seconds: int = 10) -> None:
    logger.info("Checking %s at %s:%s", service_name, host, port)
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return
    except OSError as exc:
        raise AirflowException(
            f"{service_name} is unavailable at {host}:{port}"
        ) from exc


def _check_http_service(service_name: str, url: str) -> None:
    logger.info("Checking %s at %s", service_name, url)
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise AirflowException(f"{service_name} is unavailable at {url}") from exc


def _hive_connection():
    return hive.connect(
        host=AIRFLOW_SPARK_THRIFTSERVER_HOST,
        port=AIRFLOW_SPARK_THRIFTSERVER_PORT,
        auth="NOSASL",
        database=AIRFLOW_DBT_DATABASE,
    )


def _fetch_single_value(cursor, query: str) -> Any:
    cursor.execute(query)
    row = cursor.fetchone()
    return row[0] if row else None


def _qualified_name(schema: str, relation: str) -> str:
    return f"`{schema}`.`{relation}`"


def _run_results_paths(run_id: str) -> list[Path]:
    if not REPORTS_RUNS_DIR.exists():
        return []
    safe_prefix = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in run_id)
    return sorted(REPORTS_RUNS_DIR.glob(f"{safe_prefix}_*_run_results.json"))


def _count_failed_tests_from_results(run_id: str) -> int:
    failed_tests = 0
    for result_path in _run_results_paths(run_id):
        payload = json.loads(result_path.read_text())
        for result in payload.get("results", []):
            if result.get("resource_type") != "test":
                continue
            status = str(result.get("status", "")).lower()
            if status not in {"success", "pass", "skipped"}:
                failed_tests += 1
    return failed_tests


def check_hdfs_available(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    host, port = _parse_host_port(AIRFLOW_HDFS_URI, 9000)
    _check_tcp_service("HDFS NameNode", host, port)
    return {"run_id": run_id, "host": host, "port": port}


def check_spark_master_available(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    host, port = _parse_host_port(AIRFLOW_SPARK_MASTER_URL, 7077)
    _check_tcp_service("Spark master", host, port)
    return {"run_id": run_id, "host": host, "port": port}


def check_thriftserver_available(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    _check_tcp_service(
        "Spark ThriftServer",
        AIRFLOW_SPARK_THRIFTSERVER_HOST,
        AIRFLOW_SPARK_THRIFTSERVER_PORT,
    )
    return {
        "run_id": run_id,
        "host": AIRFLOW_SPARK_THRIFTSERVER_HOST,
        "port": AIRFLOW_SPARK_THRIFTSERVER_PORT,
    }


def check_dbt_target_connection(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    connection = _hive_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("select 1")
        row = cursor.fetchone()
        if row is None or int(row[0]) != 1:
            raise AirflowException("dbt target connection did not return a healthy result")
        return {
            "run_id": run_id,
            "database": AIRFLOW_DBT_DATABASE,
            "result": int(row[0]),
        }
    finally:
        cursor.close()
        connection.close()


def check_superset_available(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    health_url = urljoin(
        AIRFLOW_SUPERSET_URL.rstrip("/") + "/",
        AIRFLOW_SUPERSET_HEALTH_ENDPOINT.lstrip("/"),
    )
    _check_http_service("Superset", health_url)
    return {"run_id": run_id, "health_url": health_url}


def validate_source_files(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    csv_names = _load_csv_names()
    if not csv_names:
        raise AirflowException(f"No CSV files found in {RAW_DATA_DIR}")

    logger.info("[run_id=%s] Validated %d local source files", run_id, len(csv_names))
    return {"run_id": run_id, "dataset_count": len(csv_names), "datasets": csv_names}


def run_spark_ingestion(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    logger.info("[run_id=%s] Starting Spark ingestion", run_id)

    from processing import analysis

    result = analysis.main()
    if result != 0:
        raise AirflowException("Spark ingestion failed")

    source_metadata = context["ti"].xcom_pull(
        task_ids="validation.validate_source_files",
        key="return_value",
    ) or {}
    return {
        "run_id": run_id,
        "exit_code": result,
        "dataset_count": source_metadata.get("dataset_count"),
    }


def verify_bronze_output(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    spark = None
    row_counts: Dict[str, int] = {}
    try:
        spark = create_spark_session()
        dataset_names = _load_csv_names()
        for dataset_name in dataset_names:
            parquet_uri = f"{AIRFLOW_HDFS_URI.rstrip('/')}/olist/parquet/{dataset_name}"
            row_counts[dataset_name] = spark.read.parquet(parquet_uri).count()
        if not row_counts:
            raise AirflowException("No bronze Parquet outputs were verified")

        source_metadata = context["ti"].xcom_pull(
            task_ids="validation.validate_source_files",
            key="return_value",
        ) or {}
        dataset_count = source_metadata.get("dataset_count")
        if dataset_count is not None and dataset_count != len(row_counts):
            raise AirflowException(
                "Bronze output count does not match the validated source count"
            )

        logger.info("[run_id=%s] Verified bronze outputs: %s", run_id, row_counts)
        return {
            "run_id": run_id,
            "verified_datasets": len(row_counts),
            "row_counts": row_counts,
        }
    finally:
        if spark is not None:
            spark.stop()


def _dbt_env() -> Dict[str, str]:
    return {
        "AIRFLOW_DBT_TARGET": AIRFLOW_DBT_TARGET,
        "AIRFLOW_DBT_DATABASE": AIRFLOW_DBT_DATABASE,
        "AIRFLOW_DBT_SCHEMA": AIRFLOW_DBT_SCHEMA,
        "AIRFLOW_DBT_BRONZE_SCHEMA": AIRFLOW_DBT_BRONZE_SCHEMA,
        "AIRFLOW_DBT_SILVER_SCHEMA": AIRFLOW_DBT_SILVER_SCHEMA,
        "AIRFLOW_DBT_GOLD_SCHEMA": AIRFLOW_DBT_GOLD_SCHEMA,
        "AIRFLOW_DBT_THREADS": str(AIRFLOW_DBT_THREADS),
        "AIRFLOW_DBT_USER": AIRFLOW_DBT_USER,
        "AIRFLOW_SPARK_THRIFTSERVER_HOST": AIRFLOW_SPARK_THRIFTSERVER_HOST,
        "AIRFLOW_SPARK_THRIFTSERVER_PORT": str(AIRFLOW_SPARK_THRIFTSERVER_PORT),
    }


def _dbt_command(
    subcommand: str,
    select: str | None = None,
    result_suffix: str | None = None,
) -> str:
    select_clause = f" --select {select}" if select else ""
    target_clause = f' --target "${{AIRFLOW_DBT_TARGET:-{AIRFLOW_DBT_TARGET}}}"'
    results_clause = ""
    if result_suffix is not None:
        results_clause = (
            f" && mkdir -p {REPORTS_RUNS_DIR} && "
            "safe_run_id=\"${PIPELINE_RUN_ID:-unknown}\" && "
            "safe_run_id=\"${safe_run_id//[^A-Za-z0-9_.-]/_}\" && "
            f"cp target/run_results.json {REPORTS_RUNS_DIR}/${{safe_run_id}}_{result_suffix}_run_results.json"
        )
    return (
        "set -euo pipefail && "
        f'echo "[run_id=${{PIPELINE_RUN_ID:-unknown}}] dbt {subcommand}" && '
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt {subcommand} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        f"{target_clause}{select_clause}{results_clause}"
    )


def reconcile_gold_metrics(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    connection = _hive_connection()
    cursor = connection.cursor()
    try:
        bronze_source_counts: Dict[str, int] = {}
        for table_name in [
            "orders",
            "order_items",
            "order_payments",
            "order_reviews",
            "customers",
            "sellers",
            "products",
            "category_translation",
        ]:
            bronze_source_counts[table_name] = int(
                _fetch_single_value(
                    cursor,
                    f"select count(*) from {_qualified_name(AIRFLOW_DBT_BRONZE_SCHEMA, table_name)}",
                )
                or 0
            )

        silver_entity_counts: Dict[str, int] = {}
        for table_name in [
            "orders",
            "order_items",
            "order_payments",
            "order_reviews",
            "customers",
            "sellers",
            "products",
        ]:
            silver_entity_counts[table_name] = int(
                _fetch_single_value(
                    cursor,
                    f"select count(*) from {_qualified_name(AIRFLOW_DBT_SILVER_SCHEMA, table_name)}",
                )
                or 0
            )

        gold_fact_count = int(
            _fetch_single_value(
                cursor,
                f"select count(*) from {_qualified_name(AIRFLOW_DBT_GOLD_SCHEMA, 'fact_orders')}",
            )
            or 0
        )
        allocated_payment_total = float(
            _fetch_single_value(
                cursor,
                f"""
                select coalesce(cast(sum(allocated_payment_value) as double), 0.0)
                from {_qualified_name(AIRFLOW_DBT_GOLD_SCHEMA, 'fact_orders')}
                """,
            )
            or 0.0
        )
        order_payment_total = float(
            _fetch_single_value(
                cursor,
                f"""
                select coalesce(cast(sum(order_payment_value) as double), 0.0)
                from (
                    select distinct order_id, order_payment_value
                    from {_qualified_name(AIRFLOW_DBT_GOLD_SCHEMA, 'fact_orders')}
                ) distinct_order_payments
                """,
            )
            or 0.0
        )
        grain_row_count = int(
            _fetch_single_value(
                cursor,
                f"""
                select count(*)
                from (
                    select distinct order_id, order_item_id
                    from {_qualified_name(AIRFLOW_DBT_GOLD_SCHEMA, 'fact_orders')}
                ) grain_rows
                """,
            )
            or 0
        )
        failed_dbt_test_count = _count_failed_tests_from_results(run_id)

        if abs(allocated_payment_total - order_payment_total) > 0.01:
            raise AirflowException(
                "Allocated payment total does not reconcile with order-level payment total"
            )
        if gold_fact_count != grain_row_count:
            raise AirflowException(
                "Gold fact row count does not match the distinct order-item grain count"
            )

        artifact_payload = {
            "run_id": run_id,
            "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "bronze_source_counts": bronze_source_counts,
            "silver_entity_counts": silver_entity_counts,
            "gold_fact_count": gold_fact_count,
            "allocated_payment_total": allocated_payment_total,
            "order_payment_total": order_payment_total,
            "failed_dbt_test_count": failed_dbt_test_count,
        }

        REPORTS_RUNS_DIR.mkdir(parents=True, exist_ok=True)
        artifact_path = REPORTS_RUNS_DIR / (
            "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in run_id) + ".json"
        )
        artifact_path.write_text(json.dumps(artifact_payload, indent=2, sort_keys=True))

        logger.info("[run_id=%s] Wrote reconciliation artifact to %s", run_id, artifact_path)
        return {
            "run_id": run_id,
            "artifact_path": str(artifact_path),
            "gold_fact_count": gold_fact_count,
            "allocated_payment_total": allocated_payment_total,
            "failed_dbt_test_count": failed_dbt_test_count,
        }
    finally:
        cursor.close()
        connection.close()


def verify_hive_tables(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    expected_tables = [
        "fact_orders",
        "dim_customers",
        "dim_products",
        "dim_sellers",
        "dim_date",
    ]

    connection = _hive_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(f"show tables in {AIRFLOW_DBT_GOLD_SCHEMA}")
        available_tables = {row[0] for row in cursor.fetchall()}
        missing_tables = sorted(set(expected_tables) - available_tables)
        if missing_tables:
            raise AirflowException(
                f"Missing Hive tables in {AIRFLOW_DBT_GOLD_SCHEMA}: {', '.join(missing_tables)}"
            )

        return {
            "run_id": run_id,
            "database": AIRFLOW_DBT_GOLD_SCHEMA,
            "available_tables": expected_tables,
        }
    finally:
        cursor.close()
        connection.close()


def _superset_session() -> requests.Session:
    session = requests.Session()
    if AIRFLOW_SUPERSET_ACCESS_TOKEN:
        session.headers.update({"Authorization": f"Bearer {AIRFLOW_SUPERSET_ACCESS_TOKEN}"})
        return session

    if AIRFLOW_SUPERSET_USERNAME and AIRFLOW_SUPERSET_PASSWORD:
        login_url = urljoin(AIRFLOW_SUPERSET_URL.rstrip("/") + "/", "/api/v1/security/login")
        response = session.post(
            login_url,
            json={
                "username": AIRFLOW_SUPERSET_USERNAME,
                "password": AIRFLOW_SUPERSET_PASSWORD,
                "provider": "db",
                "refresh": True,
            },
            timeout=30,
        )
        response.raise_for_status()
        access_token = response.json().get("access_token")
        if not access_token:
            raise AirflowException("Superset login did not return an access token")
        session.headers.update({"Authorization": f"Bearer {access_token}"})
    return session


def refresh_superset_metadata(**context: Any) -> Dict[str, Any]:
    run_id = _pipeline_run_id(context)
    session = _superset_session()

    health_url = urljoin(
        AIRFLOW_SUPERSET_URL.rstrip("/") + "/",
        AIRFLOW_SUPERSET_HEALTH_ENDPOINT.lstrip("/"),
    )
    health_response = session.get(health_url, timeout=30)
    health_response.raise_for_status()

    refresh_url = urljoin(
        AIRFLOW_SUPERSET_URL.rstrip("/") + "/",
        AIRFLOW_SUPERSET_REFRESH_ENDPOINT.lstrip("/"),
    )
    refresh_response = session.get(refresh_url, timeout=30)
    refresh_response.raise_for_status()

    return {
        "run_id": run_id,
        "health_url": health_url,
        "refresh_url": refresh_url,
        "status_code": refresh_response.status_code,
    }


dag_doc_md = """
# Olist Medallion Pipeline

This DAG orchestrates the Phase 3 medallion rebuild with explicit preflight
checks, Spark ingestion, dbt Bronze/Silver/Gold transformations, Gold
reconciliation, Hive verification, and a safe Superset metadata refresh.

## Architecture

- **Preflight** confirms HDFS, Spark master, ThriftServer, the dbt target, and
  Superset are reachable before the main chain starts.
- **Ingestion** uses the existing Spark job as a remote Spark client boundary.
- **Bronze** stays source-aligned and minimally processed.
- **Silver** adds cleaned, typed, and deduplicated business-ready models.
- **Gold** rebuilds the Phase 2 star schema from Silver only.
- **Publication** refreshes Superset metadata through an idempotent HTTP call.

## Operator choices

- Lightweight validation uses PythonOperator.
- Spark ingestion uses a Python task boundary that launches the existing Spark
  job from the Airflow runtime.
- dbt steps use BashOperator to keep `deps`, `debug`, `run`, and `test`
  explicit and serial.
- Superset refresh uses a requests-based HTTP task so credentials stay outside
  the DAG source.

## Retry behavior

- Retries are enabled at the DAG level and apply to each task.
- Long-running tasks have explicit execution timeouts.
- The DAG is limited to one active run to keep the reconstruction chain
  serialized.

## Resources

- Spark ingestion uses the Spark cluster configured in `processing.config`.
- dbt runs stay single-threaded by DAG design and the runtime profile.
- Validation tasks stay lightweight and only return small metadata values
  through XCom.

## Recovery strategy

- Re-run the DAG from the failed task after fixing the upstream issue.
- Bronze, Silver, and Gold can be re-materialized independently.
- Reconciliation results are written to `reports/phase3/runs/` using the
  Airflow run identifier so retries stay safe and auditable.
"""


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _task_failure_callback,
}


with DAG(
    dag_id=DAG_ID,
    description="Phase 3 medallion pipeline for the Olist project",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["olist", "spark", "dbt", "medallion"],
    doc_md=dag_doc_md,
    render_template_as_native_obj=True,
) as dag:
    with TaskGroup("preflight") as preflight:
        check_hdfs_available_task = PythonOperator(
            task_id="check_hdfs_available",
            python_callable=check_hdfs_available,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

        check_spark_master_available_task = PythonOperator(
            task_id="check_spark_master_available",
            python_callable=check_spark_master_available,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

        check_thriftserver_available_task = PythonOperator(
            task_id="check_thriftserver_available",
            python_callable=check_thriftserver_available,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

        check_dbt_target_connection_task = PythonOperator(
            task_id="check_dbt_target_connection",
            python_callable=check_dbt_target_connection,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

        check_superset_available_task = PythonOperator(
            task_id="check_superset_available",
            python_callable=check_superset_available,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

    with TaskGroup("validation") as validation:
        validate_source_files_task = PythonOperator(
            task_id="validate_source_files",
            python_callable=validate_source_files,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

        verify_bronze_output_task = PythonOperator(
            task_id="verify_bronze_output",
            python_callable=verify_bronze_output,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

        reconcile_gold_metrics_task = PythonOperator(
            task_id="reconcile_gold_metrics",
            python_callable=reconcile_gold_metrics,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

        verify_hive_tables_task = PythonOperator(
            task_id="verify_hive_tables",
            python_callable=verify_hive_tables,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

    with TaskGroup("ingestion") as ingestion:
        run_spark_ingestion_task = PythonOperator(
            task_id="run_spark_ingestion",
            python_callable=run_spark_ingestion,
            execution_timeout=LONG_TASK_TIMEOUT,
        )

    with TaskGroup("dbt_setup") as dbt_setup:
        dbt_deps_task = BashOperator(
            task_id="dbt_deps",
            bash_command=(
                "set -euo pipefail && "
                "echo \"[run_id=${PIPELINE_RUN_ID:-unknown}] dbt deps\" && "
                f"cd {DBT_PROJECT_DIR} && "
                f"dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
            ),
            env={"PIPELINE_RUN_ID": "{{ run_id }}", **_dbt_env()},
            append_env=True,
            execution_timeout=LONG_TASK_TIMEOUT,
        )

        dbt_debug_task = BashOperator(
            task_id="dbt_debug",
            bash_command=_dbt_command("debug"),
            env={"PIPELINE_RUN_ID": "{{ run_id }}", **_dbt_env()},
            append_env=True,
            execution_timeout=LONG_TASK_TIMEOUT,
        )

    with TaskGroup("dbt_bronze") as dbt_bronze:
        dbt_bronze_run_task = BashOperator(
            task_id="dbt_bronze_run",
            bash_command=_dbt_command("run", "path:models/bronze"),
            env={"PIPELINE_RUN_ID": "{{ run_id }}", **_dbt_env()},
            append_env=True,
            execution_timeout=LONG_TASK_TIMEOUT,
        )

        dbt_bronze_test_task = BashOperator(
            task_id="dbt_bronze_test",
            bash_command=_dbt_command("test", "path:models/bronze", "bronze"),
            env={"PIPELINE_RUN_ID": "{{ run_id }}", **_dbt_env()},
            append_env=True,
            execution_timeout=LONG_TASK_TIMEOUT,
        )

    with TaskGroup("dbt_silver") as dbt_silver:
        dbt_silver_run_task = BashOperator(
            task_id="dbt_silver_run",
            bash_command=_dbt_command("run", "path:models/silver"),
            env={"PIPELINE_RUN_ID": "{{ run_id }}", **_dbt_env()},
            append_env=True,
            execution_timeout=LONG_TASK_TIMEOUT,
        )

        dbt_silver_test_task = BashOperator(
            task_id="dbt_silver_test",
            bash_command=_dbt_command("test", "path:models/silver", "silver"),
            env={"PIPELINE_RUN_ID": "{{ run_id }}", **_dbt_env()},
            append_env=True,
            execution_timeout=LONG_TASK_TIMEOUT,
        )

    with TaskGroup("dbt_gold") as dbt_gold:
        dbt_gold_run_task = BashOperator(
            task_id="dbt_gold_run",
            bash_command=_dbt_command("run", "path:models/gold"),
            env={"PIPELINE_RUN_ID": "{{ run_id }}", **_dbt_env()},
            append_env=True,
            execution_timeout=LONG_TASK_TIMEOUT,
        )

        dbt_gold_test_task = BashOperator(
            task_id="dbt_gold_test",
            bash_command=_dbt_command("test", "path:models/gold", "gold"),
            env={"PIPELINE_RUN_ID": "{{ run_id }}", **_dbt_env()},
            append_env=True,
            execution_timeout=LONG_TASK_TIMEOUT,
        )

    with TaskGroup("publication") as publication:
        refresh_superset_metadata_task = PythonOperator(
            task_id="refresh_superset_metadata",
            python_callable=refresh_superset_metadata,
            execution_timeout=SHORT_TASK_TIMEOUT,
        )

    preflight_tasks = [
        check_hdfs_available_task,
        check_spark_master_available_task,
        check_thriftserver_available_task,
        check_dbt_target_connection_task,
        check_superset_available_task,
    ]
    for preflight_task in preflight_tasks:
        preflight_task >> validate_source_files_task

    validate_source_files_task >> run_spark_ingestion_task >> verify_bronze_output_task
    verify_bronze_output_task >> dbt_deps_task >> dbt_debug_task >> dbt_bronze_run_task
    dbt_bronze_run_task >> dbt_bronze_test_task >> dbt_silver_run_task
    dbt_silver_run_task >> dbt_silver_test_task >> dbt_gold_run_task
    dbt_gold_run_task >> dbt_gold_test_task >> reconcile_gold_metrics_task
    reconcile_gold_metrics_task >> verify_hive_tables_task >> refresh_superset_metadata_task
