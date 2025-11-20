
-- sql/models/risk_metrics.sql (mock)
CREATE TABLE IF NOT EXISTS risk_metrics AS
SELECT p.instrument,
       p.position_qty,
       pr.close AS last_price,
       p.position_qty * pr.close AS pnl_mock
FROM positions p
JOIN stg_prices pr USING (instrument);
