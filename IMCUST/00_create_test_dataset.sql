-- ============================================
-- IMCUST (SOURCE) - Create Test Dataset
-- ============================================
-- Purpose: Create comprehensive test dataset for migration testing
-- Creates 10 tables and 2 views across two schemas with dependencies

USE ROLE ACCOUNTADMIN;
USE DATABASE PROD_DB;

-- ============================================
-- SCHEMA 1: SRC_INVESTMENTS_BOLT (Raw/Source Layer)
-- ============================================

USE SCHEMA SRC_INVESTMENTS_BOLT;

-- Table 1: Raw stock data (deepest dependency)
CREATE OR REPLACE TABLE stock_master (
    stock_id INT PRIMARY KEY,
    ticker VARCHAR(10),
    company_name VARCHAR(100),
    sector VARCHAR(50),
    industry VARCHAR(50),
    ipo_date DATE,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO stock_master VALUES
(1, 'AAPL', 'Apple Inc.', 'Technology', 'Consumer Electronics', '1980-12-12', CURRENT_TIMESTAMP()),
(2, 'MSFT', 'Microsoft Corporation', 'Technology', 'Software', '1986-03-13', CURRENT_TIMESTAMP()),
(3, 'GOOGL', 'Alphabet Inc.', 'Technology', 'Internet Services', '2004-08-19', CURRENT_TIMESTAMP()),
(4, 'AMZN', 'Amazon.com Inc.', 'Consumer Cyclical', 'E-Commerce', '1997-05-15', CURRENT_TIMESTAMP()),
(5, 'TSLA', 'Tesla Inc.', 'Consumer Cyclical', 'Auto Manufacturers', '2010-06-29', CURRENT_TIMESTAMP());

-- Table 2: Raw price data
CREATE OR REPLACE TABLE stock_prices_raw (
    price_id INT PRIMARY KEY,
    stock_id INT,
    price_date DATE,
    open_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    volume BIGINT,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO stock_prices_raw VALUES
(1, 1, '2024-01-01', 185.00, 188.50, 189.00, 184.50, 52000000, CURRENT_TIMESTAMP()),
(2, 1, '2024-01-02', 188.50, 190.00, 191.00, 187.50, 48000000, CURRENT_TIMESTAMP()),
(3, 2, '2024-01-01', 370.00, 375.00, 376.00, 369.00, 28000000, CURRENT_TIMESTAMP()),
(4, 2, '2024-01-02', 375.00, 378.50, 380.00, 374.00, 26000000, CURRENT_TIMESTAMP()),
(5, 3, '2024-01-01', 140.00, 142.50, 143.00, 139.50, 22000000, CURRENT_TIMESTAMP()),
(6, 3, '2024-01-02', 142.50, 145.00, 146.00, 141.50, 24000000, CURRENT_TIMESTAMP()),
(7, 4, '2024-01-01', 155.00, 157.50, 158.00, 154.50, 45000000, CURRENT_TIMESTAMP()),
(8, 4, '2024-01-02', 157.50, 160.00, 161.00, 156.50, 42000000, CURRENT_TIMESTAMP()),
(9, 5, '2024-01-01', 238.00, 242.00, 244.00, 237.00, 95000000, CURRENT_TIMESTAMP()),
(10, 5, '2024-01-02', 242.00, 245.50, 248.00, 240.00, 98000000, CURRENT_TIMESTAMP());

-- Table 3: Raw transaction data
CREATE OR REPLACE TABLE transactions_raw (
    transaction_id INT PRIMARY KEY,
    stock_id INT,
    transaction_date DATE,
    transaction_type VARCHAR(4), -- 'BUY' or 'SELL'
    quantity INT,
    price_per_share DECIMAL(10,2),
    total_amount DECIMAL(15,2),
    broker_id INT,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO transactions_raw VALUES
(1, 1, '2024-01-01', 'BUY', 100, 188.50, 18850.00, 101, CURRENT_TIMESTAMP()),
(2, 2, '2024-01-01', 'BUY', 50, 375.00, 18750.00, 102, CURRENT_TIMESTAMP()),
(3, 3, '2024-01-01', 'BUY', 75, 142.50, 10687.50, 101, CURRENT_TIMESTAMP()),
(4, 4, '2024-01-02', 'BUY', 80, 160.00, 12800.00, 103, CURRENT_TIMESTAMP()),
(5, 5, '2024-01-02', 'SELL', 25, 245.50, 6137.50, 102, CURRENT_TIMESTAMP()),
(6, 1, '2024-01-02', 'SELL', 30, 190.00, 5700.00, 101, CURRENT_TIMESTAMP());

-- Table 4: Broker information
CREATE OR REPLACE TABLE broker_master (
    broker_id INT PRIMARY KEY,
    broker_name VARCHAR(100),
    contact_email VARCHAR(100),
    commission_rate DECIMAL(5,4),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO broker_master VALUES
(101, 'TD Ameritrade', 'support@tdameritrade.com', 0.0025, CURRENT_TIMESTAMP()),
(102, 'E*TRADE', 'help@etrade.com', 0.0030, CURRENT_TIMESTAMP()),
(103, 'Charles Schwab', 'contact@schwab.com', 0.0020, CURRENT_TIMESTAMP());

-- Table 5: Customer accounts
CREATE OR REPLACE TABLE customer_accounts (
    account_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    account_type VARCHAR(20), -- 'INDIVIDUAL', 'JOINT', 'RETIREMENT'
    broker_id INT,
    balance DECIMAL(15,2),
    opened_date DATE,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO customer_accounts VALUES
(1001, 'John Smith', 'INDIVIDUAL', 101, 50000.00, '2023-01-15', CURRENT_TIMESTAMP()),
(1002, 'Jane Doe', 'RETIREMENT', 102, 75000.00, '2023-02-20', CURRENT_TIMESTAMP()),
(1003, 'Bob Johnson', 'JOINT', 103, 100000.00, '2023-03-10', CURRENT_TIMESTAMP()),
(1004, 'Alice Williams', 'INDIVIDUAL', 101, 45000.00, '2023-04-05', CURRENT_TIMESTAMP());

-- ============================================
-- SCHEMA 2: MART_INVESTMENTS_BOLT (Business Logic Layer)
-- ============================================

USE SCHEMA MART_INVESTMENTS_BOLT;

-- Table 6: Dimension table for stocks (references SRC layer)
CREATE OR REPLACE TABLE dim_stocks (
    stock_key INT PRIMARY KEY,
    stock_id INT,
    ticker VARCHAR(10),
    company_name VARCHAR(100),
    sector VARCHAR(50),
    industry VARCHAR(50),
    effective_from_date DATE,
    effective_to_date DATE,
    is_current BOOLEAN,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO dim_stocks
SELECT
    stock_id as stock_key,
    stock_id,
    ticker,
    company_name,
    sector,
    industry,
    ipo_date as effective_from_date,
    '9999-12-31'::DATE as effective_to_date,
    TRUE as is_current,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_INVESTMENTS_BOLT.stock_master;

-- Table 7: Dimension table for brokers (references SRC layer)
CREATE OR REPLACE TABLE dim_brokers (
    broker_key INT PRIMARY KEY,
    broker_id INT,
    broker_name VARCHAR(100),
    contact_email VARCHAR(100),
    commission_rate DECIMAL(5,4),
    effective_from_date DATE,
    is_active BOOLEAN,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO dim_brokers
SELECT
    broker_id as broker_key,
    broker_id,
    broker_name,
    contact_email,
    commission_rate,
    CURRENT_DATE() as effective_from_date,
    TRUE as is_active,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_INVESTMENTS_BOLT.broker_master;

-- Table 8: Fact table for transactions (references SRC and MART layers)
CREATE OR REPLACE TABLE fact_transactions (
    fact_transaction_id INT PRIMARY KEY,
    transaction_id INT,
    stock_key INT,
    broker_key INT,
    transaction_date DATE,
    transaction_type VARCHAR(4),
    quantity INT,
    price_per_share DECIMAL(10,2),
    total_amount DECIMAL(15,2),
    commission_amount DECIMAL(10,2),
    net_amount DECIMAL(15,2),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO fact_transactions
SELECT
    t.transaction_id as fact_transaction_id,
    t.transaction_id,
    t.stock_id as stock_key,
    t.broker_id as broker_key,
    t.transaction_date,
    t.transaction_type,
    t.quantity,
    t.price_per_share,
    t.total_amount,
    ROUND(t.total_amount * b.commission_rate, 2) as commission_amount,
    CASE
        WHEN t.transaction_type = 'BUY' THEN ROUND(t.total_amount + (t.total_amount * b.commission_rate), 2)
        ELSE ROUND(t.total_amount - (t.total_amount * b.commission_rate), 2)
    END as net_amount,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_INVESTMENTS_BOLT.transactions_raw t
JOIN PROD_DB.SRC_INVESTMENTS_BOLT.broker_master b ON t.broker_id = b.broker_id;

-- Table 9: Aggregated daily prices (references SRC layer)
CREATE OR REPLACE TABLE daily_stock_performance (
    performance_id INT PRIMARY KEY,
    stock_id INT,
    price_date DATE,
    open_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    daily_change DECIMAL(10,2),
    daily_change_pct DECIMAL(6,2),
    volume BIGINT,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO daily_stock_performance
SELECT
    ROW_NUMBER() OVER (ORDER BY stock_id, price_date) as performance_id,
    stock_id,
    price_date,
    open_price,
    close_price,
    ROUND(close_price - open_price, 2) as daily_change,
    ROUND(((close_price - open_price) / open_price) * 100, 2) as daily_change_pct,
    volume,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_INVESTMENTS_BOLT.stock_prices_raw;

-- Table 10: Portfolio summary (references multiple layers)
CREATE OR REPLACE TABLE portfolio_summary (
    portfolio_id INT PRIMARY KEY,
    account_id INT,
    stock_id INT,
    total_shares_owned INT,
    average_purchase_price DECIMAL(10,2),
    total_invested DECIMAL(15,2),
    last_updated_ts TIMESTAMP_LTZ,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO portfolio_summary
SELECT
    ROW_NUMBER() OVER (ORDER BY ca.account_id, t.stock_id) as portfolio_id,
    ca.account_id,
    t.stock_id,
    SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.quantity ELSE -t.quantity END) as total_shares_owned,
    ROUND(AVG(CASE WHEN t.transaction_type = 'BUY' THEN t.price_per_share ELSE NULL END), 2) as average_purchase_price,
    SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.total_amount ELSE 0 END) as total_invested,
    MAX(t.created_ts) as last_updated_ts,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_INVESTMENTS_BOLT.customer_accounts ca
CROSS JOIN PROD_DB.SRC_INVESTMENTS_BOLT.transactions_raw t
WHERE ca.account_id IN (1001, 1002, 1003)  -- Sample accounts
GROUP BY ca.account_id, t.stock_id
HAVING total_shares_owned > 0;

-- ============================================
-- SRC SCHEMA VIEWS - Base Layer (Query SRC tables)
-- ============================================
-- These views in SRC schema will be discovered as upstream dependencies
-- They have UNQUALIFIED table references (same schema) to test Fix #2.1

USE SCHEMA SRC_INVESTMENTS_BOLT;

-- SRC View 1: Stock Master Enhanced (unqualified references to test Fix #2.1)
CREATE OR REPLACE VIEW vw_stock_master_enhanced AS
SELECT
    sm.stock_id,
    sm.ticker,
    sm.company_name,
    sm.sector,
    sm.industry,
    sm.ipo_date,
    -- Add price statistics from unqualified table reference
    COUNT(spr.price_date) as trading_days,
    AVG(spr.close_price) as avg_price,
    MAX(spr.close_price) as max_price,
    MIN(spr.close_price) as min_price
FROM stock_master sm                    -- ❌ Unqualified (tests Fix #2.1)
LEFT JOIN stock_prices_raw spr          -- ❌ Unqualified (tests Fix #2.1)
    ON sm.stock_id = spr.stock_id
GROUP BY sm.stock_id, sm.ticker, sm.company_name, sm.sector, sm.industry, sm.ipo_date;

-- SRC View 2: Broker Master Enhanced (unqualified references)
CREATE OR REPLACE VIEW vw_broker_master_enhanced AS
SELECT
    bm.broker_id,
    bm.broker_name,
    bm.contact_email,
    bm.commission_rate,
    -- Add account statistics from unqualified table reference
    COUNT(DISTINCT ca.account_id) as total_accounts,
    SUM(ca.balance) as total_balance_managed
FROM broker_master bm                   -- ❌ Unqualified (tests Fix #2.1)
LEFT JOIN customer_accounts ca          -- ❌ Unqualified (tests Fix #2.1)
    ON bm.broker_id = ca.broker_id
GROUP BY bm.broker_id, bm.broker_name, bm.contact_email, bm.commission_rate;

-- SRC View 3: Transaction Metrics (unqualified references)
CREATE OR REPLACE VIEW vw_transaction_metrics AS
SELECT
    tr.transaction_id,
    tr.transaction_date,
    tr.transaction_type,
    tr.broker_id,
    tr.stock_id,
    tr.quantity,
    tr.price_per_share,
    tr.total_amount,
    -- Cross-reference with stock master
    sm.ticker,
    sm.company_name,
    sm.sector
FROM transactions_raw tr                -- ❌ Unqualified (tests Fix #2.1)
JOIN stock_master sm                    -- ❌ Unqualified (tests Fix #2.1)
    ON tr.stock_id = sm.stock_id;

USE SCHEMA MART_INVESTMENTS_BOLT;

-- ============================================
-- MART INTERMEDIATE VIEWS - Layer 2 (Query MART tables + SRC views)
-- ============================================
-- These views reference SRC VIEWS (cross-schema view dependencies)
-- This creates: MART views → SRC views → SRC tables dependency chain

-- Intermediate View 1: Enriched Transactions (now references SRC view)
CREATE OR REPLACE VIEW vw_enriched_transactions AS
SELECT
    ft.fact_transaction_id,
    ft.transaction_date,
    ft.transaction_type,
    ft.stock_key,
    ft.broker_key,
    ft.quantity,
    ft.price_per_share,
    ft.total_amount,
    ft.commission_amount,
    ft.net_amount,
    -- Add calculated fields
    ROUND(ft.commission_amount / ft.total_amount * 100, 2) as commission_rate_pct,
    CASE WHEN ft.transaction_type = 'BUY' THEN 1 ELSE -1 END as direction_multiplier
FROM PROD_DB.MART_INVESTMENTS_BOLT.fact_transactions ft;

-- Intermediate View 2: Stock Dimensions (now references SRC VIEW instead of table)
CREATE OR REPLACE VIEW vw_stock_dimensions AS
SELECT
    ds.stock_key,
    ds.ticker,
    ds.company_name,
    ds.sector,
    ds.is_current,
    -- Cross-schema reference to SRC VIEW (not table directly)
    sme.stock_id as source_stock_id,
    sme.ipo_date,
    sme.industry,
    sme.avg_price,
    sme.trading_days
FROM PROD_DB.MART_INVESTMENTS_BOLT.dim_stocks ds
LEFT JOIN PROD_DB.SRC_INVESTMENTS_BOLT.vw_stock_master_enhanced sme  -- ✅ References SRC VIEW
    ON ds.ticker = sme.ticker;

-- Intermediate View 3: Broker Information (now references SRC VIEW instead of table)
CREATE OR REPLACE VIEW vw_broker_info AS
SELECT
    db.broker_key,
    db.broker_name,
    db.commission_rate,
    db.is_active,
    -- Cross-schema reference to SRC VIEW (not table directly)
    bme.broker_id as source_broker_id,
    bme.contact_email,
    bme.total_accounts,
    bme.total_balance_managed
FROM PROD_DB.MART_INVESTMENTS_BOLT.dim_brokers db
LEFT JOIN PROD_DB.SRC_INVESTMENTS_BOLT.vw_broker_master_enhanced bme  -- ✅ References SRC VIEW
    ON db.broker_name = bme.broker_name;

-- Intermediate View 4: Daily Performance Metrics
CREATE OR REPLACE VIEW vw_daily_performance AS
SELECT
    dsp.stock_id,
    dsp.price_date,
    dsp.open_price,
    dsp.close_price,
    dsp.daily_change,
    dsp.daily_change_pct,
    dsp.volume
FROM PROD_DB.MART_INVESTMENTS_BOLT.daily_stock_performance dsp;

-- ============================================
-- FINAL VIEWS - Layer 2 (Query intermediate views)
-- ============================================
-- These are the views you request in migration
-- They depend on VIEWS (not tables), testing view-on-view dependencies

-- View 1: Transaction Analysis (DEPENDS ON 3 INTERMEDIATE VIEWS)
-- This view queries other VIEWS, not tables directly
CREATE OR REPLACE VIEW vw_transaction_analysis AS
SELECT
    et.fact_transaction_id,
    et.transaction_date,
    et.transaction_type,
    et.quantity,
    et.price_per_share,
    et.total_amount,
    et.commission_amount,
    et.net_amount,
    et.commission_rate_pct,
    -- From vw_stock_dimensions (VIEW dependency)
    sd.ticker,
    sd.company_name,
    sd.sector,
    sd.source_stock_id,
    -- From vw_broker_info (VIEW dependency)
    bi.broker_name,
    bi.commission_rate as broker_commission_rate,
    bi.is_active as broker_is_active,
    bi.contact_email as broker_email,
    -- From vw_daily_performance (VIEW dependency)
    dp.daily_change_pct,
    dp.volume
FROM PROD_DB.MART_INVESTMENTS_BOLT.vw_enriched_transactions et
JOIN PROD_DB.MART_INVESTMENTS_BOLT.vw_stock_dimensions sd
    ON et.stock_key = sd.stock_key
JOIN PROD_DB.MART_INVESTMENTS_BOLT.vw_broker_info bi
    ON et.broker_key = bi.broker_key
LEFT JOIN PROD_DB.MART_INVESTMENTS_BOLT.vw_daily_performance dp
    ON et.stock_key = dp.stock_id
    AND et.transaction_date = dp.price_date;

-- View 2: Portfolio performance (complex multi-table join)
CREATE OR REPLACE VIEW vw_portfolio_performance AS
SELECT
    ps.portfolio_id,
    ca.account_id,
    ca.customer_name,
    ca.account_type,
    ds.ticker,
    ds.company_name,
    ps.total_shares_owned,
    ps.average_purchase_price,
    ps.total_invested,
    ROUND(ps.total_shares_owned * spr.close_price, 2) as current_value,
    ROUND((ps.total_shares_owned * spr.close_price) - ps.total_invested, 2) as unrealized_gain_loss,
    ROUND((((ps.total_shares_owned * spr.close_price) - ps.total_invested) / ps.total_invested) * 100, 2) as return_pct,
    db.broker_name
FROM portfolio_summary ps
JOIN PROD_DB.SRC_INVESTMENTS_BOLT.customer_accounts ca ON ps.account_id = ca.account_id
JOIN dim_stocks ds ON ps.stock_id = ds.stock_key
JOIN dim_brokers db ON ca.broker_id = db.broker_key
JOIN PROD_DB.SRC_INVESTMENTS_BOLT.stock_prices_raw spr
    ON ps.stock_id = spr.stock_id
    AND spr.price_date = (
        SELECT MAX(price_date)
        FROM PROD_DB.SRC_INVESTMENTS_BOLT.stock_prices_raw
        WHERE stock_id = ps.stock_id
    );

-- ============================================
-- ADDITIONAL VIEWS - Multi-Level View Dependencies
-- ============================================
-- These views create a dependency hierarchy to properly test:
-- 1. View-depends-on-view scenarios
-- 2. Dependency level ordering
-- 3. Cross-schema references at multiple levels

-- View 3: Stock Performance Summary (depends on SRC tables directly)
-- This is a base-level view that other views will depend on
CREATE OR REPLACE VIEW vw_stock_performance_summary AS
SELECT
    sm.stock_id,
    sm.ticker,
    sm.company_name,
    sm.sector,
    COUNT(DISTINCT spr.price_date) as trading_days,
    MIN(spr.close_price) as min_price,
    MAX(spr.close_price) as max_price,
    AVG(spr.close_price) as avg_price,
    ROUND(AVG(spr.volume), 0) as avg_volume
FROM PROD_DB.SRC_INVESTMENTS_BOLT.stock_master sm
LEFT JOIN PROD_DB.SRC_INVESTMENTS_BOLT.stock_prices_raw spr
    ON sm.stock_id = spr.stock_id
GROUP BY sm.stock_id, sm.ticker, sm.company_name, sm.sector;

-- View 4: Trading Summary (depends on vw_transaction_analysis - VIEW-on-VIEW dependency)
-- This tests that views can depend on other views
CREATE OR REPLACE VIEW vw_trading_summary AS
SELECT
    ta.ticker,
    ta.company_name,
    ta.sector,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN ta.transaction_type = 'BUY' THEN 1 ELSE 0 END) as buy_count,
    SUM(CASE WHEN ta.transaction_type = 'SELL' THEN 1 ELSE 0 END) as sell_count,
    SUM(ta.quantity) as total_quantity,
    ROUND(AVG(ta.price_per_share), 2) as avg_price_per_share,
    ROUND(SUM(ta.total_amount), 2) as total_amount,
    ROUND(SUM(ta.commission_amount), 2) as total_commission,
    ROUND(AVG(ta.daily_change_pct), 2) as avg_daily_change_pct
FROM PROD_DB.MART_INVESTMENTS_BOLT.vw_transaction_analysis ta
GROUP BY ta.ticker, ta.company_name, ta.sector;

-- View 5: Final Dashboard (depends on MULTIPLE views - complex VIEW-on-VIEW dependencies)
-- This is the top-level view that ties everything together
CREATE OR REPLACE VIEW vw_final_investment_dashboard AS
SELECT
    ts.ticker,
    ts.company_name,
    ts.sector,
    ts.total_transactions,
    ts.buy_count,
    ts.sell_count,
    ts.avg_price_per_share,
    ts.total_commission,
    -- Cross-reference with stock performance summary
    sps.trading_days,
    sps.min_price,
    sps.max_price,
    sps.avg_price as market_avg_price,
    sps.avg_volume as market_avg_volume,
    -- Calculate performance metrics
    ROUND(((ts.avg_price_per_share - sps.avg_price) / sps.avg_price) * 100, 2) as price_variance_pct
FROM PROD_DB.MART_INVESTMENTS_BOLT.vw_trading_summary ts
LEFT JOIN PROD_DB.MART_INVESTMENTS_BOLT.vw_stock_performance_summary sps
    ON ts.ticker = sps.ticker;

-- View 6: Portfolio Value Tracker (depends on vw_portfolio_performance - another VIEW-on-VIEW)
-- This adds another branch to the view dependency tree
CREATE OR REPLACE VIEW vw_portfolio_value_tracker AS
SELECT
    pp.customer_name,
    pp.account_type,
    COUNT(DISTINCT pp.ticker) as num_holdings,
    ROUND(SUM(pp.total_invested), 2) as total_invested,
    ROUND(SUM(pp.current_value), 2) as total_current_value,
    ROUND(SUM(pp.unrealized_gain_loss), 2) as total_unrealized_gain_loss,
    ROUND(AVG(pp.return_pct), 2) as avg_return_pct,
    -- Add reference to broker from cross-schema
    pp.broker_name
FROM PROD_DB.MART_INVESTMENTS_BOLT.vw_portfolio_performance pp
GROUP BY pp.customer_name, pp.account_type, pp.broker_name;

-- ============================================
-- Verification Queries
-- ============================================

-- Verify table row counts
SELECT 'SRC_INVESTMENTS_BOLT' as schema_name, 'stock_master' as table_name, COUNT(*) as row_count FROM PROD_DB.SRC_INVESTMENTS_BOLT.stock_master
UNION ALL
SELECT 'SRC_INVESTMENTS_BOLT', 'stock_prices_raw', COUNT(*) FROM PROD_DB.SRC_INVESTMENTS_BOLT.stock_prices_raw
UNION ALL
SELECT 'SRC_INVESTMENTS_BOLT', 'transactions_raw', COUNT(*) FROM PROD_DB.SRC_INVESTMENTS_BOLT.transactions_raw
UNION ALL
SELECT 'SRC_INVESTMENTS_BOLT', 'broker_master', COUNT(*) FROM PROD_DB.SRC_INVESTMENTS_BOLT.broker_master
UNION ALL
SELECT 'SRC_INVESTMENTS_BOLT', 'customer_accounts', COUNT(*) FROM PROD_DB.SRC_INVESTMENTS_BOLT.customer_accounts
UNION ALL
SELECT 'MART_INVESTMENTS_BOLT', 'dim_stocks', COUNT(*) FROM PROD_DB.MART_INVESTMENTS_BOLT.dim_stocks
UNION ALL
SELECT 'MART_INVESTMENTS_BOLT', 'dim_brokers', COUNT(*) FROM PROD_DB.MART_INVESTMENTS_BOLT.dim_brokers
UNION ALL
SELECT 'MART_INVESTMENTS_BOLT', 'fact_transactions', COUNT(*) FROM PROD_DB.MART_INVESTMENTS_BOLT.fact_transactions
UNION ALL
SELECT 'MART_INVESTMENTS_BOLT', 'daily_stock_performance', COUNT(*) FROM PROD_DB.MART_INVESTMENTS_BOLT.daily_stock_performance
UNION ALL
SELECT 'MART_INVESTMENTS_BOLT', 'portfolio_summary', COUNT(*) FROM PROD_DB.MART_INVESTMENTS_BOLT.portfolio_summary
ORDER BY schema_name, table_name;

-- Verify views
SELECT 'vw_transaction_analysis' as view_name, COUNT(*) as row_count
FROM PROD_DB.MART_INVESTMENTS_BOLT.vw_transaction_analysis
UNION ALL
SELECT 'vw_portfolio_performance', COUNT(*)
FROM PROD_DB.MART_INVESTMENTS_BOLT.vw_portfolio_performance;

-- Show all objects created
SHOW TABLES IN SCHEMA PROD_DB.SRC_INVESTMENTS_BOLT;
SHOW TABLES IN SCHEMA PROD_DB.MART_INVESTMENTS_BOLT;
SHOW VIEWS IN SCHEMA PROD_DB.MART_INVESTMENTS_BOLT;
