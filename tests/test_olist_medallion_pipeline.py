import ast
from pathlib import Path

import pytest

try:
    from airflow.models.dagbag import DagBag
except ModuleNotFoundError:
    DagBag = None


AIRFLOW_AVAILABLE = DagBag is not None


DAG_ID = "olist_medallion_pipeline"
DAG_FILE = Path("airflow/dags/olist_medallion_pipeline.py")
SOURCE_TEXT = DAG_FILE.read_text()


def load_dag():
    if not AIRFLOW_AVAILABLE or DagBag is None:
        pytest.skip("apache-airflow is not installed in this environment")
    dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)
    assert dagbag.import_errors == {}
    dag = dagbag.get_dag(DAG_ID)
    assert dag is not None
    return dag


def test_dag_imports_without_errors():
    load_dag()


def test_expected_task_ids_exist():
    dag = load_dag()
    expected_task_ids = {
        "preflight.check_hdfs_available",
        "preflight.check_spark_master_available",
        "preflight.check_thriftserver_available",
        "preflight.check_dbt_target_connection",
        "preflight.check_superset_available",
        "validation.validate_source_files",
        "ingestion.run_spark_ingestion",
        "validation.verify_bronze_output",
        "dbt_setup.dbt_deps",
        "dbt_setup.dbt_debug",
        "dbt_bronze.dbt_bronze_run",
        "dbt_bronze.dbt_bronze_test",
        "dbt_silver.dbt_silver_run",
        "dbt_silver.dbt_silver_test",
        "dbt_gold.dbt_gold_run",
        "dbt_gold.dbt_gold_test",
        "validation.reconcile_gold_metrics",
        "validation.verify_hive_tables",
        "publication.refresh_superset_metadata",
    }
    assert expected_task_ids == set(dag.task_ids)


def test_dependency_chain_is_correct():
    dag = load_dag()

    def upstream(task_id):
        return dag.get_task(task_id).upstream_task_ids

    assert upstream("validation.validate_source_files") == {
        "preflight.check_hdfs_available",
        "preflight.check_spark_master_available",
        "preflight.check_thriftserver_available",
        "preflight.check_dbt_target_connection",
        "preflight.check_superset_available",
    }
    assert upstream("ingestion.run_spark_ingestion") == {
        "validation.validate_source_files"
    }
    assert upstream("validation.verify_bronze_output") == {
        "ingestion.run_spark_ingestion"
    }
    assert upstream("dbt_setup.dbt_deps") == {
        "validation.verify_bronze_output"
    }
    assert upstream("dbt_setup.dbt_debug") == {
        "dbt_setup.dbt_deps"
    }
    assert upstream("dbt_bronze.dbt_bronze_run") == {
        "dbt_setup.dbt_debug"
    }
    assert upstream("dbt_bronze.dbt_bronze_test") == {
        "dbt_bronze.dbt_bronze_run"
    }
    assert upstream("dbt_silver.dbt_silver_run") == {
        "dbt_bronze.dbt_bronze_test"
    }
    assert upstream("dbt_silver.dbt_silver_test") == {
        "dbt_silver.dbt_silver_run"
    }
    assert upstream("dbt_gold.dbt_gold_run") == {
        "dbt_silver.dbt_silver_test"
    }
    assert upstream("dbt_gold.dbt_gold_test") == {
        "dbt_gold.dbt_gold_run"
    }
    assert upstream("validation.reconcile_gold_metrics") == {
        "dbt_gold.dbt_gold_test"
    }
    assert upstream("validation.verify_hive_tables") == {
        "validation.reconcile_gold_metrics"
    }
    assert upstream("publication.refresh_superset_metadata") == {
        "validation.verify_hive_tables"
    }


def test_dag_configuration():
    dag = load_dag()
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert str(dag.schedule_interval) == "@daily"
    assert set(dag.tags) == {"olist", "spark", "dbt", "medallion"}


def test_no_sensitive_strings_are_hard_coded():
    forbidden_fragments = {
        "admin@local.test",
        "change-me-for-local-demo",
        "superset_password",
        "airflow:airflow",
        "AIRFLOW_ADMIN_PASSWORD",
    }
    assert not any(fragment in SOURCE_TEXT for fragment in forbidden_fragments)


def test_dag_source_is_valid_python():
    ast.parse(SOURCE_TEXT)
