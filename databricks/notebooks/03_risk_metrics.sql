
-- Databricks SQL — 03_risk_metrics.sql
-- Gold layer (mock PnL / VaR placeholder)
CREATE OR REPLACE TABLE gold.risk_metrics AS
SELECT
  p.instrument,
  p.position_qty,
  pr.close as last_price,
  p.position_qty * pr.close AS pnl_mock
FROM silver.positions p
JOIN bronze.prices pr USING (instrument);
