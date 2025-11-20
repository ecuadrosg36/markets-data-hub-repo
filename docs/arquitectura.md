
# Arquitectura de Referencia (Markets Data Hub · Market Risk)

```mermaid
flowchart LR
  A[Sources: Trades, Prices, RefData] -->|Land| B[GCS/ADLS Raw]
  B -->|Ingest (Spark)| C[Bronze (Delta)]
  C -->|Transform (dbt/SQL/Spark)| D[Silver: Positions]
  D -->|Aggregate| E[Gold: Risk Metrics (VaR mock, PnL)]
  E -->|Publish| F[Consumers (Risk, Reporting)]
  subgraph Orchestration
    G[Airflow DAG] --> H[Databricks Jobs]
    H --> I[Control-M Job-as-Code]
  end
  E --> J[GE/Tests/Monitoring]
```
