-- ============================================
-- IMCUST (SOURCE) - Create Test Dataset V2 (Different Objects)
-- ============================================
-- Purpose: Create a NEW test dataset with different objects to test:
--   1. Objects with NO query history (tests fallback strategies)
--   2. Objects with multiple query histories (tests selection logic)
--   3. Complex transformations (tests SQL adaptation)
--   4. Different dependency patterns
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE PROD_DB;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS SRC_ORDERS_BOLT;
CREATE SCHEMA IF NOT EXISTS MART_ORDERS_BOLT;

USE SCHEMA SRC_ORDERS_BOLT;

-- ============================================
-- BASE TABLES (Level 2 - deepest dependencies)
-- ============================================

-- Table 1: Product Catalog (base table)
CREATE OR REPLACE TABLE product_catalog (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10,2),
    supplier_id INT,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO product_catalog VALUES
(1, 'Laptop Pro 15', 'Electronics', 1299.99, 101, CURRENT_TIMESTAMP()),
(2, 'Wireless Mouse', 'Electronics', 29.99, 101, CURRENT_TIMESTAMP()),
(3, 'Office Chair', 'Furniture', 199.99, 102, CURRENT_TIMESTAMP()),
(4, 'Desk Lamp', 'Furniture', 49.99, 102, CURRENT_TIMESTAMP()),
(5, 'Notebook Set', 'Stationery', 15.99, 103, CURRENT_TIMESTAMP());

-- Table 2: Customer Master (base table)
CREATE OR REPLACE TABLE customer_master (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    region VARCHAR(50),
    customer_tier VARCHAR(20),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO customer_master VALUES
(1001, 'Alice Johnson', 'alice@email.com', 'North', 'PREMIUM', CURRENT_TIMESTAMP()),
(1002, 'Bob Smith', 'bob@email.com', 'South', 'STANDARD', CURRENT_TIMESTAMP()),
(1003, 'Charlie Brown', 'charlie@email.com', 'East', 'PREMIUM', CURRENT_TIMESTAMP()),
(1004, 'Diana Prince', 'diana@email.com', 'West', 'STANDARD', CURRENT_TIMESTAMP()),
(1005, 'Eve Wilson', 'eve@email.com', 'North', 'PREMIUM', CURRENT_TIMESTAMP());

-- Table 3: Order Headers (base table - will have NO query history)
CREATE OR REPLACE TABLE order_headers_raw (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    order_status VARCHAR(20),
    total_amount DECIMAL(15,2),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO order_headers_raw VALUES
(5001, 1001, '2024-01-15', 'COMPLETED', 1329.98, CURRENT_TIMESTAMP()),
(5002, 1002, '2024-01-16', 'COMPLETED', 229.98, CURRENT_TIMESTAMP()),
(5003, 1003, '2024-01-17', 'PENDING', 1249.98, CURRENT_TIMESTAMP()),
(5004, 1004, '2024-01-18', 'COMPLETED', 249.98, CURRENT_TIMESTAMP()),
(5005, 1005, '2024-01-19', 'COMPLETED', 1315.98, CURRENT_TIMESTAMP());

-- Table 4: Order Lines (base table)
CREATE OR REPLACE TABLE order_lines_raw (
    line_id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(15,2),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO order_lines_raw VALUES
(10001, 5001, 1, 1, 1299.99, 1299.99, CURRENT_TIMESTAMP()),
(10002, 5001, 2, 1, 29.99, 29.99, CURRENT_TIMESTAMP()),
(10003, 5002, 3, 1, 199.99, 199.99, CURRENT_TIMESTAMP()),
(10004, 5002, 2, 1, 29.99, 29.99, CURRENT_TIMESTAMP()),
(10005, 5003, 1, 1, 1299.99, 1299.99, CURRENT_TIMESTAMP()),
(10006, 5004, 4, 1, 49.99, 49.99, CURRENT_TIMESTAMP()),
(10007, 5004, 3, 1, 199.99, 199.99, CURRENT_TIMESTAMP()),
(10008, 5005, 1, 1, 1299.99, 1299.99, CURRENT_TIMESTAMP()),
(10009, 5005, 5, 1, 15.99, 15.99, CURRENT_TIMESTAMP());

-- ============================================
-- DERIVED TABLES (Level 1 - have transformations)
-- ============================================

USE SCHEMA MART_ORDERS_BOLT;

-- Table 1: Dim Products (derived from product_catalog)
-- This will have query history captured
CREATE OR REPLACE TABLE dim_products (
    product_key INT PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10,2),
    supplier_id INT,
    effective_from_date DATE,
    effective_to_date DATE,
    is_current BOOLEAN,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert using transformation SQL (will be captured in query history)
INSERT INTO dim_products
SELECT
    product_id as product_key,
    product_id,
    product_name,
    category,
    unit_price,
    supplier_id,
    CURRENT_DATE() as effective_from_date,
    '9999-12-31'::DATE as effective_to_date,
    TRUE as is_current,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_ORDERS_BOLT.product_catalog;

-- Table 2: Dim Customers (derived from customer_master)
-- This will have MULTIPLE query histories (we'll insert twice)
CREATE OR REPLACE TABLE dim_customers (
    customer_key INT PRIMARY KEY,
    customer_id INT,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    region VARCHAR(50),
    customer_tier VARCHAR(20),
    effective_from_date DATE,
    is_active BOOLEAN,
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- First insert (older - will be ignored)
INSERT INTO dim_customers
SELECT
    customer_id as customer_key,
    customer_id,
    customer_name,
    email,
    region,
    customer_tier,
    CURRENT_DATE() - 1 as effective_from_date,
    TRUE as is_active,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_ORDERS_BOLT.customer_master
WHERE customer_tier = 'PREMIUM';

-- Second insert (newer - will be selected by framework)
INSERT INTO dim_customers
SELECT
    customer_id as customer_key,
    customer_id,
    customer_name,
    email,
    region,
    customer_tier,
    CURRENT_DATE() as effective_from_date,
    TRUE as is_active,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_ORDERS_BOLT.customer_master;

-- Table 3: Fact Orders (complex transformation - will have query history)
CREATE OR REPLACE TABLE fact_orders (
    fact_order_id INT PRIMARY KEY,
    order_id INT,
    customer_key INT,
    product_key INT,
    order_date DATE,
    order_status VARCHAR(20),
    quantity INT,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(15,2),
    discount_amount DECIMAL(10,2),
    net_amount DECIMAL(15,2),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Complex transformation with joins and calculations
INSERT INTO fact_orders
SELECT
    ol.line_id as fact_order_id,
    ol.order_id,
    dc.customer_key,
    dp.product_key,
    oh.order_date,
    oh.order_status,
    ol.quantity,
    ol.unit_price,
    ol.line_total,
    CASE
        WHEN dc.customer_tier = 'PREMIUM' THEN ROUND(ol.line_total * 0.10, 2)
        ELSE 0.00
    END as discount_amount,
    CASE
        WHEN dc.customer_tier = 'PREMIUM' THEN ROUND(ol.line_total * 0.90, 2)
        ELSE ol.line_total
    END as net_amount,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_ORDERS_BOLT.order_lines_raw ol
JOIN PROD_DB.SRC_ORDERS_BOLT.order_headers_raw oh ON ol.order_id = oh.order_id
JOIN PROD_DB.SRC_ORDERS_BOLT.customer_master cm ON oh.customer_id = cm.customer_id
JOIN dim_customers dc ON cm.customer_id = dc.customer_id
JOIN dim_products dp ON ol.product_id = dp.product_id;

-- Table 4: Sales Summary (will have NO query history - tests fallback)
-- We'll create it but NOT populate it via INSERT (simulates missing history)
CREATE OR REPLACE TABLE sales_summary (
    summary_id INT PRIMARY KEY,
    order_date DATE,
    region VARCHAR(50),
    category VARCHAR(50),
    total_orders INT,
    total_revenue DECIMAL(15,2),
    avg_order_value DECIMAL(10,2),
    created_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Manually populate (simulates data loaded via external tool, no query history)
INSERT INTO sales_summary VALUES
(1, '2024-01-15', 'North', 'Electronics', 1, 1329.98, 1329.98, CURRENT_TIMESTAMP()),
(2, '2024-01-16', 'South', 'Furniture', 1, 229.98, 229.98, CURRENT_TIMESTAMP()),
(3, '2024-01-17', 'East', 'Electronics', 1, 1249.98, 1249.98, CURRENT_TIMESTAMP());

-- Add COMMENT to sales_summary (tests fallback Strategy 2)
COMMENT ON TABLE sales_summary IS 
'INSERT INTO sales_summary
SELECT
    ROW_NUMBER() OVER (ORDER BY oh.order_date, cm.region, dp.category) as summary_id,
    oh.order_date,
    cm.region,
    dp.category,
    COUNT(DISTINCT oh.order_id) as total_orders,
    SUM(ol.line_total) as total_revenue,
    ROUND(AVG(ol.line_total), 2) as avg_order_value,
    CURRENT_TIMESTAMP() as created_ts
FROM PROD_DB.SRC_ORDERS_BOLT.order_headers_raw oh
JOIN PROD_DB.SRC_ORDERS_BOLT.order_lines_raw ol ON oh.order_id = ol.order_id
JOIN PROD_DB.SRC_ORDERS_BOLT.customer_master cm ON oh.customer_id = cm.customer_id
JOIN dim_products dp ON ol.product_id = dp.product_id
GROUP BY oh.order_date, cm.region, dp.category';

-- Table 5: Monthly Revenue (will use CTAS - tests CTAS to INSERT conversion)
CREATE OR REPLACE TABLE monthly_revenue AS
SELECT
    DATE_TRUNC('MONTH', fo.order_date) as month,
    dc.region,
    SUM(fo.net_amount) as total_revenue,
    COUNT(DISTINCT fo.order_id) as order_count,
    ROUND(AVG(fo.net_amount), 2) as avg_order_value,
    CURRENT_TIMESTAMP() as created_ts
FROM fact_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
GROUP BY DATE_TRUNC('MONTH', fo.order_date), dc.region;

-- ============================================
-- VIEWS
-- ============================================

CREATE OR REPLACE VIEW vw_order_details AS
SELECT
    fo.fact_order_id,
    fo.order_id,
    fo.order_date,
    dc.customer_name,
    dc.region,
    dp.product_name,
    dp.category,
    fo.quantity,
    fo.unit_price,
    fo.discount_amount,
    fo.net_amount
FROM fact_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
JOIN dim_products dp ON fo.product_key = dp.product_key;

CREATE OR REPLACE VIEW vw_customer_orders AS
SELECT
    dc.customer_key,
    dc.customer_name,
    dc.region,
    dc.customer_tier,
    COUNT(DISTINCT fo.order_id) as total_orders,
    SUM(fo.net_amount) as total_spent,
    ROUND(AVG(fo.net_amount), 2) as avg_order_value
FROM dim_customers dc
LEFT JOIN fact_orders fo ON dc.customer_key = fo.customer_key
GROUP BY dc.customer_key, dc.customer_name, dc.region, dc.customer_tier;

-- ============================================
-- Summary
-- ============================================

SELECT 'SRC_ORDERS_BOLT' as schema_name, 'product_catalog' as table_name, COUNT(*) as row_count FROM PROD_DB.SRC_ORDERS_BOLT.product_catalog
UNION ALL
SELECT 'SRC_ORDERS_BOLT', 'customer_master', COUNT(*) FROM PROD_DB.SRC_ORDERS_BOLT.customer_master
UNION ALL
SELECT 'SRC_ORDERS_BOLT', 'order_headers_raw', COUNT(*) FROM PROD_DB.SRC_ORDERS_BOLT.order_headers_raw
UNION ALL
SELECT 'SRC_ORDERS_BOLT', 'order_lines_raw', COUNT(*) FROM PROD_DB.SRC_ORDERS_BOLT.order_lines_raw
UNION ALL
SELECT 'MART_ORDERS_BOLT', 'dim_products', COUNT(*) FROM PROD_DB.MART_ORDERS_BOLT.dim_products
UNION ALL
SELECT 'MART_ORDERS_BOLT', 'dim_customers', COUNT(*) FROM PROD_DB.MART_ORDERS_BOLT.dim_customers
UNION ALL
SELECT 'MART_ORDERS_BOLT', 'fact_orders', COUNT(*) FROM PROD_DB.MART_ORDERS_BOLT.fact_orders
UNION ALL
SELECT 'MART_ORDERS_BOLT', 'sales_summary', COUNT(*) FROM PROD_DB.MART_ORDERS_BOLT.sales_summary
UNION ALL
SELECT 'MART_ORDERS_BOLT', 'monthly_revenue', COUNT(*) FROM PROD_DB.MART_ORDERS_BOLT.monthly_revenue
ORDER BY schema_name, table_name;

