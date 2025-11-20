-- Markets Data Hub - SQL Models
-- Dimensional model for Market Risk analytics

-- =====================================================
-- DIMENSION: Trades
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_trades (
    trade_id STRING NOT NULL,
    trade_date DATE NOT NULL,
    settlement_date DATE,
    instrument_id STRING NOT NULL,
    counterparty_id STRING NOT NULL,
    trade_type STRING NOT NULL,  -- 'BUY', 'SELL'
    quantity DECIMAL(18,4) NOT NULL,
    price DECIMAL(18,4) NOT NULL,
    currency STRING NOT NULL,
    trader_id STRING,
    book_id STRING,
    status STRING,  -- 'OPEN', 'SETTLED', 'CANCELLED'
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (trade_date)
COMMENT 'Trade dimension - all trade transactions';

-- =====================================================
-- DIMENSION: Instruments
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_instruments (
    instrument_id STRING NOT NULL,
    instrument_name STRING NOT NULL,
    instrument_type STRING NOT NULL,  -- 'EQUITY', 'BOND', 'DERIVATIVE', 'FX'
    asset_class STRING,  -- 'EQUITY', 'FIXED_INCOME', 'COMMODITY', 'FX'
    sector STRING,
    issuer STRING,
    currency STRING NOT NULL,
    maturity_date DATE,
    coupon_rate DECIMAL(10,4),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Instrument master data';

-- =====================================================
-- DIMENSION: Counterparties
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_counterparties (
    counterparty_id STRING NOT NULL,
    counterparty_name STRING NOT NULL,
    counterparty_type STRING,  -- 'BANK', 'BROKER', 'CORPORATE', 'FUND'
    country STRING,
    sector STRING,
    credit_rating STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Counterparty master data';

-- =====================================================
-- FACT: Market Prices
-- =====================================================
CREATE TABLE IF NOT EXISTS fct_market_prices (
    price_id STRING NOT NULL,
    price_date DATE NOT NULL,
    instrument_id STRING NOT NULL,
    open_price DECIMAL(18,4),
    high_price DECIMAL(18,4),
    low_price DECIMAL(18,4),
    close_price DECIMAL(18,4) NOT NULL,
    volume BIGINT,
    bid_price DECIMAL(18,4),
    ask_price DECIMAL(18,4),
    source STRING,  -- 'BLOOMBERG', 'REUTERS', 'MANUAL'
    created_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (price_date)
COMMENT 'End-of-day market prices for all instruments';

-- =====================================================
-- FACT: Positions
-- =====================================================
CREATE TABLE IF NOT EXISTS fct_positions (
    position_id STRING NOT NULL,
    as_of_date DATE NOT NULL,
    instrument_id STRING NOT NULL,
    book_id STRING NOT NULL,
    quantity DECIMAL(18,4) NOT NULL,
    cost_basis DECIMAL(18,2),
    market_value DECIMAL(18,2),
    unrealized_pnl DECIMAL(18,2),
    currency STRING NOT NULL,
    created_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (as_of_date)
COMMENT 'Daily position snapshots across all books';

-- =====================================================
-- FACT: Risk Metrics
-- =====================================================
CREATE TABLE IF NOT EXISTS fct_risk_metrics (
    metric_id STRING NOT NULL,
    as_of_date DATE NOT NULL,
    instrument_id STRING,
    book_id STRING,
    counterparty_id STRING,
    
    -- Value at Risk metrics
    var_95 DECIMAL(18,2),  -- VaR at 95% confidence
    var_99 DECIMAL(18,2),  -- VaR at 99% confidence
    cvar_95 DECIMAL(18,2),  -- Conditional VaR (Expected Shortfall) at 95%
    cvar_99 DECIMAL(18,2),  -- Conditional VaR at 99%
    
    -- Greeks (for derivatives)
    delta DECIMAL(18,6),
    gamma DECIMAL(18,6),
    vega DECIMAL(18,6),
    theta DECIMAL(18,6),
    rho DECIMAL(18,6),
    
    -- P&L
    daily_pnl DECIMAL(18,2),
    mtd_pnl DECIMAL(18,2),
    ytd_pnl DECIMAL(18,2),
    
    -- Exposure
    notional_exposure DECIMAL(18,2),
    market_exposure DECIMAL(18,2),
    credit_exposure DECIMAL(18,2),
    
    created_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (as_of_date)
COMMENT 'Daily risk metrics including VaR, Greeks, P&L, and exposures';

-- =====================================================
-- Aggregated View: Portfolio Summary
-- =====================================================
CREATE OR REPLACE VIEW vw_portfolio_summary AS
SELECT
    p.as_of_date,
    i.asset_class,
    i.sector,
    COUNT(DISTINCT p.instrument_id) as instrument_count,
    SUM(p.market_value) as total_market_value,
    SUM(p.unrealized_pnl) as total_unrealized_pnl,
    AVG(r.var_95) as avg_var_95,
    SUM(r.daily_pnl) as total_daily_pnl
FROM fct_positions p
LEFT JOIN dim_instruments i ON p.instrument_id = i.instrument_id
LEFT JOIN fct_risk_metrics r ON p.instrument_id = r.instrument_id 
    AND p.as_of_date = r.as_of_date
GROUP BY p.as_of_date, i.asset_class, i.sector;

-- =====================================================
-- Aggregated View: Counterparty Risk
-- =====================================================
CREATE OR REPLACE VIEW vw_counterparty_risk AS
SELECT
    r.as_of_date,
    c.counterparty_id,
    c.counterparty_name,
    c.credit_rating,
    SUM(r.credit_exposure) as total_credit_exposure,
    AVG(r.var_95) as avg_var_95
FROM fct_risk_metrics r
LEFT JOIN dim_counterparties c ON r.counterparty_id = c.counterparty_id
GROUP BY r.as_of_date, c.counterparty_id, c.counterparty_name, c.credit_rating;
