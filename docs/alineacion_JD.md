
# Alineación con la JD — Markets Data Hub (Market Risk)

**Responsabilidades clave** | **Dónde verlo en el repo**
---|---
Definición de procesos (no solo dev) | `docs/arquitectura.md`, `job_as_code/`, `airflow/dags/markets_data_hub_etl.py`
Preparación de datos | `databricks/notebooks/01_ingest_prices.scala`, `02_trades_to_positions.py`, `sql/models/`
Creación de requests/procedimientos | `docs/procedimientos_operativos.md`, `job_as_code/controlm/flow.yaml` (formularios/campos)
Automatización por despliegues (Job-as-Code) | `job_as_code/databricks/job.json`, `job_as_code/controlm/flow.yaml`
Ejecución y validación de pruebas integradas | `tests/`, `data_quality/`, `scripts/validate_data.py`, GE suites
SQL avanzado y modelado | `sql/models/*.sql`, `sql/validations/*.sql`
Control‑M y Job‑as‑Code | `job_as_code/controlm/flow.yaml` (flows, calendarios, dependencias)
Spark/Databricks/Airflow (deseable) | `databricks/notebooks/` + `airflow/dags/`
Sector bancario y métricas de Market Risk | `docs/market_risk_basics.md` + `sql/models/risk_metrics.sql`
