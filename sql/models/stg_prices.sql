
-- sql/models/stg_prices.sql
CREATE TABLE IF NOT EXISTS stg_prices AS
SELECT * FROM read_csv_auto('data/prices.csv');
