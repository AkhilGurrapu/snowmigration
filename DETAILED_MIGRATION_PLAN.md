# Snowflake Cross-Account Migration: Detailed Implementation Plan
## IMCUST → IMSDLC Database Object Migration

**Document Version:** 1.0
**Created:** 2025-01-09
**Validated Against:** Snowflake Documentation (via MCP Server)

---

## Executive Summary

This document provides a comprehensive, step-by-step plan for migrating selected database objects from the IMCUST production account to the IMSDLC development account within the same Snowflake organization. This is a **one-time migration** with **complete dependency resolution** and **data validation**.

### Migration Scope

**Source: IMCUST Account**
- Database: `prod_db`
- Schemas: `mart_investments_bolt`, `src_investments_bolt`
- Total Objects: 8 (5 tables + 1 view + 2 stored procedures)

**Target: IMSDLC Account**
- Database: `dev_db` (already exists)
- Schemas: Same naming (already exist)
- Requirement: Full DDL + Data migration

### Objects to Migrate

| Schema | Object Name | Type | Dependencies |
|--------|-------------|------|--------------|
| SRC_INVESTMENTS_BOLT | stock_metadata_raw | TABLE | None (base table) |
| MART_INVESTMENTS_BOLT | dim_stocks | TABLE | May depend on SRC |
| MART_INVESTMENTS_BOLT | dim_portfolios | TABLE | Independent |
| MART_INVESTMENTS_BOLT | fact_transactions | TABLE | Depends on dimensions |
| MART_INVESTMENTS_BOLT | fact_daily_positions | TABLE | Depends on transactions |
| MART_INVESTMENTS_BOLT | vw_current_holdings | VIEW | Depends on tables |
| MART_INVESTMENTS_BOLT | sp_load_dim_stocks | PROCEDURE | Depends on tables |
| MART_INVESTMENTS_BOLT | sp_calculate_daily_positions | PROCEDURE | Depends on tables |

---

## Architecture & Approach

### Core Strategy: Secure Data Sharing

**Why Data Sharing?**
- ✅ Zero-copy architecture (no storage duplication)
- ✅ Near-instantaneous data access
- ✅ No egress costs (same organization)
- ✅ Enterprise-grade security
- ✅ No external stages required

**Migration Flow:**
```
IMCUST (Source)                    IMSDLC (Target)
================                   ===============
1. Create SHARE
2. Grant USAGE on objects      →   3. Consume SHARE
4. Extract DDL (GET_DDL)       →   5. Transform DDL (prod_db → dev_db)
                                   6. Create Objects (tables, views, procedures)
                                   7. Populate Data (CTAS from shared objects)
                                   8. Validate & Test
```

### Key Snowflake Features Used (All Validated)

1. **Secure Data Sharing** - Read-only cross-account data access
2. **GET_DDL()** - Extract object definitions
3. **CREATE TABLE AS SELECT (CTAS)** - Efficient data population
4. **ACCOUNT_USAGE.OBJECT_DEPENDENCIES** - Dependency tracking
5. **INFORMATION_SCHEMA** - Metadata discovery

---

## Phase-by-Phase Implementation

### Phase 1: Discovery & Inventory (IMCUST)

**Objective:** Understand what we're migrating

**Scripts:**
- `IMCUST/01_discovery.sql` (exists - needs testing)
- `IMCUST/02_dependencies.sql` (exists - needs testing)

**Actions:**
1. Query INFORMATION_SCHEMA for object metadata
2. Get row counts for all tables
3. Identify clustering keys and constraints
4. Map dependencies using ACCOUNT_USAGE.OBJECT_DEPENDENCIES
5. Validate view definitions
6. Extract procedure signatures

**Expected Outputs:**
- Complete object inventory
- Row count baselines
- Dependency graph
- External dependency warnings (if any)

**Validation Criteria:**
- All 8 objects discovered
- No missing dependencies
- Row counts captured for validation

---

### Phase 2: Data Share Setup

**Objective:** Enable cross-account data access

#### 2A: Create Share in IMCUST

**Script:** `IMCUST/03_create_share.sql`

**Validated Syntax:**
```sql
-- Step 1: Create empty share
CREATE SHARE IF NOT EXISTS migration_share_imcust_to_imsdlc
    COMMENT = 'One-time migration share: prod_db objects to dev_db';

-- Step 2: Grant database usage
GRANT USAGE ON DATABASE prod_db TO SHARE migration_share_imcust_to_imsdlc;

-- Step 3: Grant schema usage
GRANT USAGE ON SCHEMA prod_db.src_investments_bolt
    TO SHARE migration_share_imcust_to_imsdlc;
GRANT USAGE ON SCHEMA prod_db.mart_investments_bolt
    TO SHARE migration_share_imcust_to_imsdlc;

-- Step 4: Grant SELECT on tables (READ-ONLY)
GRANT SELECT ON TABLE prod_db.src_investments_bolt.stock_metadata_raw
    TO SHARE migration_share_imcust_to_imsdlc;
GRANT SELECT ON TABLE prod_db.mart_investments_bolt.dim_stocks
    TO SHARE migration_share_imcust_to_imsdlc;
GRANT SELECT ON TABLE prod_db.mart_investments_bolt.dim_portfolios
    TO SHARE migration_share_imcust_to_imsdlc;
GRANT SELECT ON TABLE prod_db.mart_investments_bolt.fact_transactions
    TO SHARE migration_share_imcust_to_imsdlc;
GRANT SELECT ON TABLE prod_db.mart_investments_bolt.fact_daily_positions
    TO SHARE migration_share_imcust_to_imsdlc;

-- Step 5: Add IMSDLC account to share
-- CRITICAL: Use organization_name.account_name format
ALTER SHARE migration_share_imcust_to_imsdlc
    ADD ACCOUNTS = nfmyizv.imsdlc;

-- Step 6: Verify share
SHOW GRANTS TO SHARE migration_share_imcust_to_imsdlc;
SHOW GRANTS OF SHARE migration_share_imcust_to_imsdlc;
```

**Key Points:**
- Views/procedures NOT shared (only DDL extracted)
- Only tables shared for data access
- Share is READ-ONLY by design

#### 2B: Consume Share in IMSDLC

**Script:** `IMSDLC/01_consume_share.sql`

**Validated Syntax:**
```sql
-- Step 1: Verify share visibility
SHOW SHARES;

-- Step 2: Describe share before consuming
DESCRIBE SHARE nfmyizv.imcust.migration_share_imcust_to_imsdlc;

-- Step 3: Create database from share
CREATE DATABASE IF NOT EXISTS migration_shared_db
    FROM SHARE nfmyizv.imcust.migration_share_imcust_to_imsdlc
    COMMENT = 'Temporary shared database for one-time migration';

-- Step 4: Verify access
SHOW SCHEMAS IN DATABASE migration_shared_db;
SHOW TABLES IN SCHEMA migration_shared_db.src_investments_bolt;
SHOW TABLES IN SCHEMA migration_shared_db.mart_investments_bolt;

-- Step 5: Test read access and get row counts
SELECT COUNT(*) FROM migration_shared_db.src_investments_bolt.stock_metadata_raw;
SELECT COUNT(*) FROM migration_shared_db.mart_investments_bolt.dim_stocks;
SELECT COUNT(*) FROM migration_shared_db.mart_investments_bolt.dim_portfolios;
SELECT COUNT(*) FROM migration_shared_db.mart_investments_bolt.fact_transactions;
SELECT COUNT(*) FROM migration_shared_db.mart_investments_bolt.fact_daily_positions;
```

**Validation:**
- Share visible in IMSDLC
- Database created successfully
- All 5 tables accessible
- Row counts match IMCUST discovery

---

### Phase 3: DDL Extraction & Transformation

**Objective:** Get object definitions and adapt for target environment

#### 3A: Extract DDL in IMCUST

**Script:** `IMCUST/04_extract_ddl.sql`

**Validated Syntax:**
```sql
-- Extract table DDL
SELECT 'STOCK_METADATA_RAW' AS object_name,
       'TABLE' AS object_type,
       GET_DDL('TABLE', 'prod_db.src_investments_bolt.stock_metadata_raw') AS ddl_statement;

SELECT 'DIM_STOCKS' AS object_name,
       'TABLE' AS object_type,
       GET_DDL('TABLE', 'prod_db.mart_investments_bolt.dim_stocks') AS ddl_statement;

SELECT 'DIM_PORTFOLIOS' AS object_name,
       'TABLE' AS object_type,
       GET_DDL('TABLE', 'prod_db.mart_investments_bolt.dim_portfolios') AS ddl_statement;

SELECT 'FACT_TRANSACTIONS' AS object_name,
       'TABLE' AS object_type,
       GET_DDL('TABLE', 'prod_db.mart_investments_bolt.fact_transactions') AS ddl_statement;

SELECT 'FACT_DAILY_POSITIONS' AS object_name,
       'TABLE' AS object_type,
       GET_DDL('TABLE', 'prod_db.mart_investments_bolt.fact_daily_positions') AS ddl_statement;

-- Extract view DDL
SELECT 'VW_CURRENT_HOLDINGS' AS object_name,
       'VIEW' AS object_type,
       GET_DDL('VIEW', 'prod_db.mart_investments_bolt.vw_current_holdings') AS ddl_statement;

-- Extract procedure DDL (with argument signatures)
SELECT 'SP_LOAD_DIM_STOCKS' AS object_name,
       'PROCEDURE' AS object_type,
       procedure_signature,
       GET_DDL('PROCEDURE', 'prod_db.mart_investments_bolt.' || procedure_signature) AS ddl_statement
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema = 'MART_INVESTMENTS_BOLT'
  AND procedure_name = 'SP_LOAD_DIM_STOCKS';

SELECT 'SP_CALCULATE_DAILY_POSITIONS' AS object_name,
       'PROCEDURE' AS object_type,
       procedure_signature,
       GET_DDL('PROCEDURE', 'prod_db.mart_investments_bolt.' || procedure_signature) AS ddl_statement
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema = 'MART_INVESTMENTS_BOLT'
  AND procedure_name = 'SP_CALCULATE_DAILY_POSITIONS';
```

#### 3B: Transform DDL

**Script:** `IMCUST/05_transform_ddl.py` (Python)

**Transformation Rules:**
1. Replace `PROD_DB` → `DEV_DB` (case-insensitive)
2. Replace `prod_db` → `dev_db` (lowercase)
3. Validate transformed SQL syntax
4. Generate executable SQL files

**Manual Review Required:**
- Verify view references are correct
- Check procedure logic references
- Validate clustering keys preserved
- Confirm constraints maintained

---

### Phase 4: Object Creation in IMSDLC

**Objective:** Create empty object structures

#### 4A: Create Tables

**Script:** `IMSDLC/02_create_tables.sql`

**Approach:**
```sql
-- OPTION 1: Use transformed DDL from GET_DDL
-- (Paste transformed DDL here)

-- OPTION 2: Create empty tables with LIMIT 0
CREATE OR REPLACE TABLE dev_db.src_investments_bolt.stock_metadata_raw AS
SELECT * FROM migration_shared_db.src_investments_bolt.stock_metadata_raw
WHERE 1=0;  -- Creates structure only, no data

-- Repeat for all tables with dependency order:
-- 1. stock_metadata_raw (SRC)
-- 2. dim_stocks (MART)
-- 3. dim_portfolios (MART)
-- 4. fact_transactions (MART)
-- 5. fact_daily_positions (MART)
```

**Validation:**
- All tables created
- Column data types match source
- Clustering keys preserved (if any)
- No data in tables yet

#### 4B: Create Views

**Script:** `IMSDLC/03_create_views.sql`

```sql
-- Use transformed view DDL
-- Verify all table references point to dev_db

-- Example structure:
CREATE OR REPLACE VIEW dev_db.mart_investments_bolt.vw_current_holdings AS
SELECT <columns>
FROM dev_db.mart_investments_bolt.fact_daily_positions fdp
JOIN dev_db.mart_investments_bolt.dim_stocks ds ON fdp.stock_id = ds.stock_id
-- ... (actual definition from GET_DDL transformation)
```

**Validation:**
- View created successfully
- View is queryable (will return 0 rows until data populated)
- No broken references

#### 4C: Create Stored Procedures

**Script:** `IMSDLC/04_create_procedures.sql`

```sql
-- Use transformed procedure DDL
-- Update all internal references to dev_db

-- Example structure:
CREATE OR REPLACE PROCEDURE dev_db.mart_investments_bolt.sp_load_dim_stocks(...)
RETURNS ...
LANGUAGE SQL
AS
$$
BEGIN
    -- Procedure logic with dev_db references
END;
$$;

-- Repeat for sp_calculate_daily_positions
```

**Validation:**
- Procedures created successfully
- Procedure signatures match source
- Internal references updated to dev_db

---

### Phase 5: Data Migration

**Objective:** Populate tables with production data

**Script:** `IMSDLC/05_populate_data.sql`

**Validated CTAS Approach:**
```sql
-- IMPORTANT: Populate in dependency order to avoid FK violations

-- 1. Base table (SRC schema)
INSERT INTO dev_db.src_investments_bolt.stock_metadata_raw
SELECT * FROM migration_shared_db.src_investments_bolt.stock_metadata_raw;

-- Verify
SELECT 'stock_metadata_raw' AS table_name, COUNT(*) AS row_count
FROM dev_db.src_investments_bolt.stock_metadata_raw;

-- 2. Dimension tables (MART schema)
INSERT INTO dev_db.mart_investments_bolt.dim_stocks
SELECT * FROM migration_shared_db.mart_investments_bolt.dim_stocks;

INSERT INTO dev_db.mart_investments_bolt.dim_portfolios
SELECT * FROM migration_shared_db.mart_investments_bolt.dim_portfolios;

-- Verify
SELECT 'dim_stocks' AS table_name, COUNT(*) AS row_count
FROM dev_db.mart_investments_bolt.dim_stocks;
SELECT 'dim_portfolios' AS table_name, COUNT(*) AS row_count
FROM dev_db.mart_investments_bolt.dim_portfolios;

-- 3. Fact tables (MART schema)
INSERT INTO dev_db.mart_investments_bolt.fact_transactions
SELECT * FROM migration_shared_db.mart_investments_bolt.fact_transactions;

INSERT INTO dev_db.mart_investments_bolt.fact_daily_positions
SELECT * FROM migration_shared_db.mart_investments_bolt.fact_daily_positions;

-- Verify
SELECT 'fact_transactions' AS table_name, COUNT(*) AS row_count
FROM dev_db.mart_investments_bolt.fact_transactions;
SELECT 'fact_daily_positions' AS table_name, COUNT(*) AS row_count
FROM dev_db.mart_investments_bolt.fact_daily_positions;
```

**Performance Considerations:**
- Use appropriate warehouse size (ADMIN_WH)
- Consider COPY GRANTS if needed
- Monitor credit consumption

---

### Phase 6: Validation & Testing

**Objective:** Ensure data integrity and functionality

#### 6A: Data Validation

**Script:** `IMSDLC/06_validate_data.sql`

```sql
-- 1. Row count comparison (CRITICAL)
WITH source_counts AS (
    SELECT 'stock_metadata_raw' AS table_name, COUNT(*) AS count
    FROM migration_shared_db.src_investments_bolt.stock_metadata_raw
    UNION ALL
    SELECT 'dim_stocks', COUNT(*)
    FROM migration_shared_db.mart_investments_bolt.dim_stocks
    UNION ALL
    SELECT 'dim_portfolios', COUNT(*)
    FROM migration_shared_db.mart_investments_bolt.dim_portfolios
    UNION ALL
    SELECT 'fact_transactions', COUNT(*)
    FROM migration_shared_db.mart_investments_bolt.fact_transactions
    UNION ALL
    SELECT 'fact_daily_positions', COUNT(*)
    FROM migration_shared_db.mart_investments_bolt.fact_daily_positions
),
target_counts AS (
    SELECT 'stock_metadata_raw' AS table_name, COUNT(*) AS count
    FROM dev_db.src_investments_bolt.stock_metadata_raw
    UNION ALL
    SELECT 'dim_stocks', COUNT(*)
    FROM dev_db.mart_investments_bolt.dim_stocks
    UNION ALL
    SELECT 'dim_portfolios', COUNT(*)
    FROM dev_db.mart_investments_bolt.dim_portfolios
    UNION ALL
    SELECT 'fact_transactions', COUNT(*)
    FROM dev_db.mart_investments_bolt.fact_transactions
    UNION ALL
    SELECT 'fact_daily_positions', COUNT(*)
    FROM dev_db.mart_investments_bolt.fact_daily_positions
)
SELECT
    s.table_name,
    s.count AS source_count,
    t.count AS target_count,
    CASE
        WHEN s.count = t.count THEN '✅ PASS'
        ELSE '❌ FAIL - ROW COUNT MISMATCH'
    END AS validation_status
FROM source_counts s
JOIN target_counts t ON s.table_name = t.table_name
ORDER BY s.table_name;

-- 2. Sample data comparison (first 10 rows)
SELECT 'SOURCE' AS source_type, *
FROM migration_shared_db.mart_investments_bolt.dim_stocks
LIMIT 10;

SELECT 'TARGET' AS source_type, *
FROM dev_db.mart_investments_bolt.dim_stocks
LIMIT 10;

-- 3. Data type validation
SELECT
    'SOURCE' AS source_type,
    table_schema,
    table_name,
    column_name,
    data_type,
    is_nullable
FROM migration_shared_db.information_schema.columns
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY table_schema, table_name, ordinal_position;

SELECT
    'TARGET' AS source_type,
    table_schema,
    table_name,
    column_name,
    data_type,
    is_nullable
FROM dev_db.information_schema.columns
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY table_schema, table_name, ordinal_position;

-- 4. NULL value comparison
SELECT 'stock_metadata_raw' AS table_name,
       COUNT(*) - COUNT(column_name) AS null_count
FROM migration_shared_db.src_investments_bolt.stock_metadata_raw
-- Repeat for key columns
```

#### 6B: Object Testing

**Script:** `IMSDLC/07_test_objects.sql`

```sql
-- 1. Test view functionality
SELECT 'View Row Count' AS test_name, COUNT(*) AS result
FROM dev_db.mart_investments_bolt.vw_current_holdings;

-- Compare with source view row count
SELECT 'Source View Row Count' AS test_name, COUNT(*) AS result
FROM prod_db.mart_investments_bolt.vw_current_holdings;

-- 2. Test view columns
SELECT * FROM dev_db.mart_investments_bolt.vw_current_holdings LIMIT 5;

-- 3. Test stored procedures (execution)
CALL dev_db.mart_investments_bolt.sp_load_dim_stocks();
-- Verify results

CALL dev_db.mart_investments_bolt.sp_calculate_daily_positions();
-- Verify results

-- 4. Verify no broken dependencies
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM dev_db.information_schema.tables
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY table_schema, table_name;
```

---

### Phase 7: Final Verification & Cleanup

**Objective:** Confirm complete migration and clean up temporary objects

**Script:** `IMSDLC/08_verify_dependencies.sql`

```sql
-- 1. Check all dependencies resolved in IMSDLC
SELECT
    referencing_object_name,
    referencing_object_type,
    referenced_object_name,
    referenced_object_type,
    referenced_schema_name
FROM snowflake.account_usage.object_dependencies
WHERE referencing_database_name = 'DEV_DB'
  AND referencing_schema_name IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
ORDER BY referencing_object_name;

-- 2. Verify no external dependencies outside dev_db
SELECT DISTINCT
    referencing_object_name,
    referenced_database_name,
    referenced_schema_name,
    referenced_object_name
FROM snowflake.account_usage.object_dependencies
WHERE referencing_database_name = 'DEV_DB'
  AND referencing_schema_name IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
  AND referenced_database_name != 'DEV_DB'
ORDER BY referencing_object_name;

-- 3. Final object count
SELECT
    table_schema AS schema_name,
    table_type AS object_type,
    COUNT(*) AS object_count
FROM dev_db.information_schema.tables
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT')
GROUP BY table_schema, table_type
ORDER BY table_schema, table_type;

-- 4. Generate migration summary
SELECT
    '✅ Migration Complete' AS status,
    COUNT(DISTINCT CASE WHEN table_type = 'BASE TABLE' THEN table_name END) AS tables_migrated,
    COUNT(DISTINCT CASE WHEN table_type = 'VIEW' THEN table_name END) AS views_migrated,
    (SELECT COUNT(*) FROM dev_db.information_schema.procedures
     WHERE procedure_schema = 'MART_INVESTMENTS_BOLT') AS procedures_migrated
FROM dev_db.information_schema.tables
WHERE table_schema IN ('SRC_INVESTMENTS_BOLT', 'MART_INVESTMENTS_BOLT');
```

**Cleanup Actions in IMSDLC:**
```sql
-- Drop temporary shared database (after confirming all data migrated)
-- CAUTION: Only run after validation passes!
-- DROP DATABASE IF EXISTS migration_shared_db;
```

**Cleanup Actions in IMCUST:**
```sql
-- Remove IMSDLC from share
-- ALTER SHARE migration_share_imcust_to_imsdlc
--     REMOVE ACCOUNTS = nfmyizv.imsdlc;

-- Drop share (after confirming migration complete)
-- DROP SHARE IF EXISTS migration_share_imcust_to_imsdlc;
```

---

## Success Criteria Checklist

| # | Criterion | Validation Method | Status |
|---|-----------|-------------------|--------|
| 1 | All 5 tables created in dev_db | Query INFORMATION_SCHEMA | ⬜ |
| 2 | Row counts match exactly | Compare source vs target counts | ⬜ |
| 3 | View vw_current_holdings functional | Query view successfully | ⬜ |
| 4 | Stored procedures execute without errors | CALL procedures and verify | ⬜ |
| 5 | No broken dependencies | Check OBJECT_DEPENDENCIES | ⬜ |
| 6 | Data types match source | Compare INFORMATION_SCHEMA.COLUMNS | ⬜ |
| 7 | Clustering keys preserved (if any) | Verify clustering_key column | ⬜ |
| 8 | All validation tests pass | Execute 06_validate_data.sql | ⬜ |

---

## Rollback Strategy

If critical issues occur during migration:

### Immediate Rollback (Per Phase)

**Phase 4-5 Issues (Object Creation/Data Load):**
```sql
-- Drop created tables in reverse dependency order
DROP TABLE IF EXISTS dev_db.mart_investments_bolt.fact_daily_positions;
DROP TABLE IF EXISTS dev_db.mart_investments_bolt.fact_transactions;
DROP TABLE IF EXISTS dev_db.mart_investments_bolt.dim_portfolios;
DROP TABLE IF EXISTS dev_db.mart_investments_bolt.dim_stocks;
DROP TABLE IF EXISTS dev_db.src_investments_bolt.stock_metadata_raw;

-- Drop view
DROP VIEW IF EXISTS dev_db.mart_investments_bolt.vw_current_holdings;

-- Drop procedures
DROP PROCEDURE IF EXISTS dev_db.mart_investments_bolt.sp_load_dim_stocks();
DROP PROCEDURE IF EXISTS dev_db.mart_investments_bolt.sp_calculate_daily_positions();
```

**Phase 6-7 Issues (Validation Failures):**
1. Document validation failures
2. Keep objects in place for troubleshooting
3. Re-run specific validation queries
4. Fix identified issues
5. Re-validate

### Complete Rollback
```sql
-- IMSDLC: Drop shared database
DROP DATABASE IF EXISTS migration_shared_db;

-- IMCUST: Revoke share access
ALTER SHARE migration_share_imcust_to_imsdlc
    REMOVE ACCOUNTS = nfmyizv.imsdlc;
DROP SHARE IF EXISTS migration_share_imcust_to_imsdlc;
```

---

## Future Migration Process Template

This migration establishes a **repeatable pattern** for future cross-account migrations:

### Reusable Components

1. **Discovery Scripts** - Parameterize database/schema names
2. **Data Share Pattern** - Template for share creation/consumption
3. **DDL Transformation** - Python script adaptable for any database rename
4. **Validation Framework** - Row count, data type, dependency checks
5. **Testing Procedures** - Systematic object testing approach

### Parameterization for Future Use

```sql
-- Example parameterized discovery script
SET source_database = 'prod_db';
SET target_database = 'dev_db';
SET schema_list = ('schema1', 'schema2');

-- Discovery queries using variables
SELECT * FROM IDENTIFIER($source_database || '.information_schema.tables')
WHERE table_schema IN ($schema_list);
```

---

## Risk Mitigation

| Risk | Impact | Mitigation | Contingency |
|------|--------|------------|-------------|
| Row count mismatch | HIGH | Multiple validation checkpoints | Re-run data load phase |
| Broken dependencies | HIGH | Thorough dependency mapping | Manual DDL correction |
| Data type changes | MEDIUM | GET_DDL preserves types | Manual column definition |
| Large data volumes | MEDIUM | Use ADMIN_WH, monitor credits | Increase warehouse size |
| Procedure logic errors | MEDIUM | Test procedures post-migration | Review and fix procedure code |
| View reference errors | MEDIUM | Validate DDL transformation | Manual view recreation |

---

## Execution Timeline Estimate

| Phase | Estimated Duration | Dependencies |
|-------|-------------------|--------------|
| Phase 1: Discovery | 15-30 minutes | None |
| Phase 2: Data Share Setup | 10-15 minutes | Phase 1 complete |
| Phase 3: DDL Extraction | 20-30 minutes | Phase 1 complete |
| Phase 4: Object Creation | 30-45 minutes | Phase 3 complete |
| Phase 5: Data Migration | 1-4 hours* | Phase 4 complete |
| Phase 6: Validation | 30-60 minutes | Phase 5 complete |
| Phase 7: Final Verification | 15-30 minutes | Phase 6 complete |

*Depends on data volume (multi-GB expected)

**Total Estimated Duration:** 3-7 hours

---

## Post-Migration Actions

1. **Documentation:**
   - Archive all scripts with execution timestamps
   - Document any issues encountered and resolutions
   - Update team knowledge base

2. **Monitoring:**
   - Monitor dev_db object usage
   - Track query performance on migrated objects
   - Verify team access and permissions

3. **Cleanup:**
   - Drop temporary shared database (7 days after validation)
   - Drop share in IMCUST (after confirmation)
   - Archive migration logs

4. **Stakeholder Communication:**
   - Notify Analytics Platform Team of completion
   - Confirm with Product Teams for validation
   - Update DataOps Team on new objects for RBAC setup

---

## Appendix: Connection Details

### IMCUST (Source)
- Account: `nfmyizv-imcust`
- User: `svc4snowflakedeploy`
- Role: `ACCOUNTADMIN`
- Warehouse: `ADMIN_WH`
- Database: `PROD_DB`
- Auth: PAT (via `.env.imcust_pat`)

### IMSDLC (Target)
- Account: `nfmyizv-imsdlc`
- User: `svc4snowflakedeploy`
- Role: `ACCOUNTADMIN`
- Warehouse: `ADMIN_WH`
- Database: `DEV_DB`
- Auth: PAT (via `.env.imsdlc_pat`)

---

## Next Steps

1. ✅ Review and approve this plan
2. ⬜ Execute Phase 1 (Discovery) in IMCUST
3. ⬜ Validate discovery results
4. ⬜ Proceed with Phase 2 (Data Share Setup)
5. ⬜ Continue through all phases with validation checkpoints

**Ready to begin execution when approved.**
