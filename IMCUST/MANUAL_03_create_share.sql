-- ============================================================================
-- IMCUST - MANUAL SHARE CREATION SCRIPT
-- Description: Create data share and grant access to IMSDLC
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;

-- ----------------------------------------------------------------------------
-- STEP 1: Create Share
-- ----------------------------------------------------------------------------

CREATE SHARE IF NOT EXISTS MIGRATION_SHARE_IMCUST_TO_IMSDLC
    COMMENT = 'One-time migration share: PROD_DB objects to DEV_DB';

-- ----------------------------------------------------------------------------
-- STEP 2: Grant Database and Schema Usage
-- ----------------------------------------------------------------------------

GRANT USAGE ON DATABASE PROD_DB
    TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

GRANT USAGE ON SCHEMA PROD_DB.SRC_INVESTMENTS_BOLT
    TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

GRANT USAGE ON SCHEMA PROD_DB.MART_INVESTMENTS_BOLT
    TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

-- ----------------------------------------------------------------------------
-- STEP 3: Grant SELECT on Tables (Base Objects)
-- ----------------------------------------------------------------------------

GRANT SELECT ON TABLE PROD_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW
    TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

GRANT SELECT ON TABLE PROD_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS
    TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

GRANT SELECT ON TABLE PROD_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS
    TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

GRANT SELECT ON TABLE PROD_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
    TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

GRANT SELECT ON TABLE PROD_DB.MART_INVESTMENTS_BOLT.FACT_DAILY_POSITIONS
    TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

-- ----------------------------------------------------------------------------
-- STEP 4: Add IMSDLC Account to Share
-- ----------------------------------------------------------------------------

-- Format: organization_name.account_name
ALTER SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC
    ADD ACCOUNTS = nfmyizv.imsdlc;

-- ----------------------------------------------------------------------------
-- STEP 5: Verify Share Configuration
-- ----------------------------------------------------------------------------

SHOW SHARES;

SHOW GRANTS TO SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

SHOW GRANTS OF SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;

DESCRIBE SHARE MIGRATION_SHARE_IMCUST_TO_IMSDLC;
