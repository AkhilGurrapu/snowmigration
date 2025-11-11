Step 1: User Initiates Migration
CALL sp_orchestrate_migration(
    'PROD_DB',                              -- source database
    'MART_INVESTMENTS_BOLT',                -- source schema
    'DEV_DB',                               -- target database
    'MART_INVESTMENTS_BOLT',                -- target schema
    ARRAY_CONSTRUCT('VW_TRANSACTION_ANALYSIS'),  -- objects to migrate
    'PROD_TO_DEV_SHARE'                     -- share name
);
Step 2: migration_config Table (Tracks Migration Request)
Purpose: Master record for each migration execution
SELECT * FROM migration_config WHERE migration_id = 101;
migration_id	source_database	source_schema	target_database	target_schema	object_list	status	created_ts
101	PROD_DB	MART_INVESTMENTS_BOLT	DEV_DB	MART_INVESTMENTS_BOLT	["VW_TRANSACTION_ANALYSIS"]	IN_PROGRESS	2025-11-10 22:17:00
Status Flow:
Starts as IN_PROGRESS
Changes to COMPLETED when orchestration finishes
Could be FAILED if errors occur
Step 3: migration_share_objects Table (Dependency Discovery)
Purpose: Stores all objects found by GET_LINEAGE with their dependency levels
SELECT migration_id, object_name, object_type, fully_qualified_name, dependency_level
FROM migration_share_objects 
WHERE migration_id = 101
ORDER BY dependency_level DESC;
migration_id	object_name	object_type	fully_qualified_name	dependency_level
101	DIM_STOCKS	TABLE	PROD_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS	3
101	STOCK_METADATA_RAW	TABLE	PROD_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW	2
101	DIM_PORTFOLIOS	TABLE	PROD_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS	1
101	DIM_STOCKS	TABLE	PROD_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS	1
101	FACT_TRANSACTIONS	TABLE	PROD_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS	1
101	TRANSACTIONS_RAW	TABLE	PROD_DB.SRC_INVESTMENTS_BOLT.TRANSACTIONS_RAW	1
101	STOCK_PRICES_RAW	TABLE	PROD_DB.SRC_INVESTMENTS_BOLT.STOCK_PRICES_RAW	1
101	VW_TRANSACTION_ANALYSIS	VIEW	PROD_DB.MART_INVESTMENTS_BOLT.VW_TRANSACTION_ANALYSIS	0
Key Points:
dependency_level = 0: The requested object (VW_TRANSACTION_ANALYSIS)
dependency_level = 1: Direct dependencies (tables this view reads from)
dependency_level = 2: Second-level dependencies (tables that level-1 tables depend on)
dependency_level = 3: Third-level dependencies (deepest)
How it's populated: IMCUST/02_sp_get_upstream_dependencies.sql:110-118
Step 4: migration_ddl_scripts Table (DDL Generation)
Purpose: Stores CREATE statements with database name replaced
SELECT migration_id, object_name, object_type, dependency_level, 
       LEFT(source_ddl, 50) as source_ddl_preview,
       LEFT(target_ddl, 50) as target_ddl_preview
FROM migration_ddl_scripts 
WHERE migration_id = 101
ORDER BY dependency_level DESC;
migration_id	object_name	object_type	dependency_level	source_ddl_preview	target_ddl_preview
101	DIM_STOCKS	TABLE	3	create or replace TABLE PROD_DB.MART_INVESTMEN...	create or replace TABLE DEV_DB.MART_INVESTMENT...
101	STOCK_METADATA_RAW	TABLE	2	create or replace TABLE PROD_DB.SRC_INVESTMENT...	create or replace TABLE DEV_DB.SRC_INVESTMENTS...
101	DIM_PORTFOLIOS	TABLE	1	create or replace TABLE PROD_DB.MART_INVESTMEN...	create or replace TABLE DEV_DB.MART_INVESTMENT...
101	FACT_TRANSACTIONS	TABLE	1	create or replace TABLE PROD_DB.MART_INVESTMEN...	create or replace TABLE DEV_DB.MART_INVESTMENT...
Example Full DDL:
-- source_ddl (extracted from PROD_DB)
CREATE OR REPLACE TABLE PROD_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS (
    STOCK_ID NUMBER(38,0),
    TICKER VARCHAR(10),
    COMPANY_NAME VARCHAR(200),
    SECTOR VARCHAR(50)
);

-- target_ddl (database name replaced: PROD_DB → DEV_DB)
CREATE OR REPLACE TABLE DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS (
    STOCK_ID NUMBER(38,0),
    TICKER VARCHAR(10),
    COMPANY_NAME VARCHAR(200),
    SECTOR VARCHAR(50)
);
Execution Order on IMSDLC:
dependency_level = 3 executed first (DIM_STOCKS)
dependency_level = 2 executed second (STOCK_METADATA_RAW)
dependency_level = 1 executed third (DIM_PORTFOLIOS, FACT_TRANSACTIONS)
How it's populated: IMCUST/03_sp_generate_migration_scripts.sql:70-80
Step 5: migration_ctas_scripts Table (Data Copy Scripts)
Purpose: Stores CTAS statements to copy data from shared database
SELECT migration_id, object_name, execution_order, ctas_script
FROM migration_ctas_scripts 
WHERE migration_id = 101
ORDER BY execution_order DESC;
migration_id	object_name	execution_order	ctas_script
101	DIM_STOCKS	3	-- CTAS for DIM_STOCKS<br>CREATE OR REPLACE TABLE DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS AS<br>SELECT * FROM <SHARED_DB_NAME>.MART_INVESTMENTS_BOLT.DIM_STOCKS;
101	STOCK_METADATA_RAW	2	-- CTAS for STOCK_METADATA_RAW<br>CREATE OR REPLACE TABLE DEV_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW AS<br>SELECT * FROM <SHARED_DB_NAME>.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW;
101	DIM_PORTFOLIOS	1	-- CTAS for DIM_PORTFOLIOS<br>CREATE OR REPLACE TABLE DEV_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS AS<br>SELECT * FROM <SHARED_DB_NAME>.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS;
101	FACT_TRANSACTIONS	1	-- CTAS for FACT_TRANSACTIONS<br>CREATE OR REPLACE TABLE DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS AS<br>SELECT * FROM <SHARED_DB_NAME>.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS;
Note:
<SHARED_DB_NAME> is a placeholder replaced at execution time on IMSDLC
Only tables get CTAS scripts (views are not copied with CTAS)
execution_order matches dependency_level from DDL scripts
How it's populated: IMCUST/03_sp_generate_migration_scripts.sql:90-100
Step 6: Data Share Setup (sp_setup_data_share)
Creates:
Database Role: MIGRATION_101_ROLE
Share: PROD_TO_DEV_SHARE
Grants:
-- Grant SELECT on all dependency objects
GRANT SELECT ON PROD_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS TO DATABASE ROLE PROD_DB.MIGRATION_101_ROLE;
GRANT SELECT ON PROD_DB.MART_INVESTMENTS_BOLT.DIM_PORTFOLIOS TO DATABASE ROLE PROD_DB.MIGRATION_101_ROLE;
GRANT SELECT ON PROD_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS TO DATABASE ROLE PROD_DB.MIGRATION_101_ROLE;
-- ... and metadata tables

-- Grant database role to share
GRANT DATABASE ROLE PROD_DB.MIGRATION_101_ROLE TO SHARE PROD_TO_DEV_SHARE;

-- Add target account
ALTER SHARE PROD_TO_DEV_SHARE ADD ACCOUNTS = IMSDLC;
Result: IMSDLC can now create a database from this share!
Step 7: IMSDLC Execution
7a. Create Database from Share
-- On IMSDLC account
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS shared_prod_db
FROM SHARE IMCUST.PROD_TO_DEV_SHARE;

GRANT IMPORTED PRIVILEGES ON DATABASE shared_prod_db TO ROLE ACCOUNTADMIN;
7b. Execute Full Migration (v2.0 signature)
-- Note: Now uses admin_schema and includes additional parameters
CALL dev_db.admin_schema.sp_execute_full_migration(
    101,                    -- Same migration_id from IMCUST!
    'shared_prod_db',       -- Shared database name
    'ADMIN_SCHEMA',         -- Admin schema in shared DB
    'DEV_DB',              -- Target database
    'ADMIN_SCHEMA',        -- Admin schema for execution log
    TRUE                    -- Validate before CTAS
);
7c. migration_execution_log Table (Execution Tracking) - v2.0
Purpose: Logs every DDL/CTAS execution on IMSDLC (now in admin_schema)
SELECT * FROM dev_db.admin_schema.migration_execution_log
WHERE migration_id = 101
ORDER BY log_id;
log_id	migration_id	execution_phase	object_name	script_type	status	error_message	execution_time_ms	executed_at
1	101	DDL_EXECUTION	DIM_STOCKS	DDL	SUCCESS	NULL	234	2025-11-10 22:20:01
2	101	DDL_EXECUTION	STOCK_METADATA_RAW	DDL	SUCCESS	NULL	189	2025-11-10 22:20:02
3	101	DDL_EXECUTION	DIM_PORTFOLIOS	DDL	SUCCESS	NULL	156	2025-11-10 22:20:03
4	101	DDL_EXECUTION	FACT_TRANSACTIONS	DDL	SUCCESS	NULL	201	2025-11-10 22:20:04
5	101	CTAS_EXECUTION	DIM_STOCKS	CTAS	SUCCESS	NULL	1234	2025-11-10 22:20:05
6	101	CTAS_EXECUTION	STOCK_METADATA_RAW	CTAS	SUCCESS	NULL	987	2025-11-10 22:20:07
7	101	CTAS_EXECUTION	DIM_PORTFOLIOS	CTAS	SUCCESS	NULL	567	2025-11-10 22:20:09
8	101	CTAS_EXECUTION	FACT_TRANSACTIONS	CTAS	SUCCESS	NULL	2345	2025-11-10 22:20:12
If an error occurred:
log_id	migration_id	execution_phase	object_name	script_type	status	error_message	execution_time_ms
5	101	CTAS_EXECUTION	DIM_STOCKS	CTAS	FAILED	SQL compilation error: Object 'SHARED_PROD_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS' does not exist	45
Complete Data Flow Diagram
┌─────────────────────────────────────────────────────────────────┐
│ IMCUST (Source Account - PROD_DB)                               │
└─────────────────────────────────────────────────────────────────┘

Step 1: User Request
─────────────────────
sp_orchestrate_migration(..., ['VW_TRANSACTION_ANALYSIS'], ...)
                    ↓
┌──────────────────────────────────────────────────────────────┐
│ migration_config                                             │
├──────────────────────────────────────────────────────────────┤
│ migration_id: 101                                            │
│ object_list: ["VW_TRANSACTION_ANALYSIS"]                     │
│ status: IN_PROGRESS → COMPLETED                             │
└──────────────────────────────────────────────────────────────┘

Step 2: Dependency Discovery (sp_get_upstream_dependencies)
──────────────────────────────────────────────────────────
GET_LINEAGE('VW_TRANSACTION_ANALYSIS', 'TABLE', 'UPSTREAM', 5)
                    ↓
┌──────────────────────────────────────────────────────────────┐
│ migration_share_objects (8 records)                          │
├──────────────────────────────────────────────────────────────┤
│ DIM_STOCKS              | dependency_level: 3                │
│ STOCK_METADATA_RAW      | dependency_level: 2                │
│ DIM_PORTFOLIOS          | dependency_level: 1                │
│ FACT_TRANSACTIONS       | dependency_level: 1                │
│ VW_TRANSACTION_ANALYSIS | dependency_level: 0                │
└──────────────────────────────────────────────────────────────┘

Step 3: Script Generation (sp_generate_migration_scripts)
────────────────────────────────────────────────────────
GET_DDL() + Replace PROD_DB → DEV_DB
                    ↓
┌──────────────────────────────────────────────────────────────┐
│ migration_ddl_scripts (4 records)                            │
├──────────────────────────────────────────────────────────────┤
│ DIM_STOCKS       | dep_level: 3 | CREATE TABLE DEV_DB...   │
│ STOCK_METADATA   | dep_level: 2 | CREATE TABLE DEV_DB...   │
│ DIM_PORTFOLIOS   | dep_level: 1 | CREATE TABLE DEV_DB...   │
│ FACT_TRANSACTIONS| dep_level: 1 | CREATE TABLE DEV_DB...   │
└──────────────────────────────────────────────────────────────┘
                    ↓
┌──────────────────────────────────────────────────────────────┐
│ migration_ctas_scripts (4 records)                           │
├──────────────────────────────────────────────────────────────┤
│ DIM_STOCKS       | exec_order: 3 | CREATE...AS SELECT *... │
│ STOCK_METADATA   | exec_order: 2 | CREATE...AS SELECT *... │
│ DIM_PORTFOLIOS   | exec_order: 1 | CREATE...AS SELECT *... │
│ FACT_TRANSACTIONS| exec_order: 1 | CREATE...AS SELECT *... │
└──────────────────────────────────────────────────────────────┘

Step 4: Share Setup (sp_setup_data_share)
────────────────────────────────────────
                    ↓
┌──────────────────────────────────────────────────────────────┐
│ Database Role: MIGRATION_101_ROLE                            │
│ Share: PROD_TO_DEV_SHARE                                     │
│ Granted: 8 objects + 4 metadata tables                       │
│ Target Account: IMSDLC                                       │
└──────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════
           DATA SHARE (Cross-Account Boundary)
═══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────┐
│ IMSDLC (Target Account - DEV_DB)                                │
└─────────────────────────────────────────────────────────────────┘

Step 5: Create Database from Share
─────────────────────────────────
CREATE DATABASE shared_prod_db FROM SHARE IMCUST.PROD_TO_DEV_SHARE;

Step 6: Execute Migration (sp_execute_full_migration)
───────────────────────────────────────────────────
Read from: shared_prod_db.mart_investments_bolt.migration_*
                    ↓
┌──────────────────────────────────────────────────────────────┐
│ DDL Execution (sp_execute_target_ddl)                        │
├──────────────────────────────────────────────────────────────┤
│ ORDER BY dependency_level DESC                               │
│ 1. Execute DIM_STOCKS (level 3)         ✓                    │
│ 2. Execute STOCK_METADATA_RAW (level 2) ✓                    │
│ 3. Execute DIM_PORTFOLIOS (level 1)     ✓                    │
│ 4. Execute FACT_TRANSACTIONS (level 1)  ✓                    │
└──────────────────────────────────────────────────────────────┘
                    ↓
┌──────────────────────────────────────────────────────────────┐
│ CTAS Execution (sp_execute_target_ctas)                      │
├──────────────────────────────────────────────────────────────┤
│ Replace <SHARED_DB_NAME> → shared_prod_db                    │
│ ORDER BY execution_order DESC                                │
│ 1. Copy DIM_STOCKS data (level 3)       ✓ 1000 rows         │
│ 2. Copy STOCK_METADATA data (level 2)   ✓ 500 rows          │
│ 3. Copy DIM_PORTFOLIOS data (level 1)   ✓ 250 rows          │
│ 4. Copy FACT_TRANSACTIONS data (level 1)✓ 5000 rows         │
└──────────────────────────────────────────────────────────────┘
                    ↓
┌──────────────────────────────────────────────────────────────┐
│ migration_execution_log (8 records)                          │
├──────────────────────────────────────────────────────────────┤
│ All DDL executions: SUCCESS                                  │
│ All CTAS executions: SUCCESS                                 │
│ Total time: 5.2 seconds                                      │
└──────────────────────────────────────────────────────────────┘

RESULT: All objects now exist in DEV_DB with data! ✅
Summary: migration_id = 101 Links Everything
-- On IMCUST: All source-side metadata
migration_config WHERE migration_id = 101         → 1 record
migration_share_objects WHERE migration_id = 101  → 8 records
migration_ddl_scripts WHERE migration_id = 101    → 4 records
migration_ctas_scripts WHERE migration_id = 101   → 4 records

-- On IMSDLC: All target-side execution logs
migration_execution_log WHERE migration_id = 101  → 8 records (4 DDL + 4 CTAS)
The migration_id = 101 is the golden thread connecting the entire cross-account migration!