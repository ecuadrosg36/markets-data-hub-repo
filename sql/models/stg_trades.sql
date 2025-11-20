
-- sql/models/stg_trades.sql
CREATE TABLE IF NOT EXISTS stg_trades AS
SELECT * FROM read_csv_auto('data/trades.csv');  -- DuckDB local demo
