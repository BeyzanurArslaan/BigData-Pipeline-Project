# Gold Models

Gold models are the reporting layer.

This layer now rebuilds the Phase 2 star schema in dbt as the new Phase 3 transformation path.

Gold contains facts, dimensions, KPIs, and dashboard-oriented marts, while the existing Spark warehouse builders remain available for comparison during the migration.
