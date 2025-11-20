
-- sql/validations/row_counts.sql
SELECT
  (SELECT COUNT(*) FROM stg_trades) AS trades_cnt,
  (SELECT COUNT(*) FROM stg_prices) AS prices_cnt,
  (SELECT COUNT(*) FROM positions) AS positions_cnt,
  (SELECT COUNT(*) FROM risk_metrics) AS risk_cnt;
