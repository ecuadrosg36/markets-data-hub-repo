
-- sql/validations/not_nulls.sql
SELECT COUNT(*) AS null_instrument FROM stg_trades WHERE instrument IS NULL;
