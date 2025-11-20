
-- sql/models/positions.sql
CREATE TABLE IF NOT EXISTS positions AS
SELECT instrument,
       SUM(qty) AS position_qty,
       AVG(price) AS avg_price
FROM stg_trades
GROUP BY instrument;
