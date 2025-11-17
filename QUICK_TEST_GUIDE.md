# Quick Testing Guide - Migration Framework v2.1

## 🚀 Quick Start

### Deploy All Fixes (Recommended)

```bash
cd /Users/akhilgurrapu/Downloads/snowmigration
./deploy_all_fixes.sh
```

### Or Deploy Individually

```bash
# Source account only
./deploy_fixes_imcust.sh

# Target account only
./deploy_fixes_imsdlc.sh
```

---

## ✅ Quick Validation Tests

### Test 1: Source Side Migration (IMCUST)

```sql
-- Connect to IMCUST account
USE ROLE ACCOUNTADMIN;
USE DATABASE PROD_DB;
USE SCHEMA ADMIN_SCHEMA;

-- Run migration orchestration
CALL PROD_DB.ADMIN_SCHEMA.sp_orchestrate_migration(
    'PROD_DB',                          -- Source database
    'MART_INVESTMENTS_BOLT',            -- Initial schema
    'ADMIN_SCHEMA',                     -- Admin schema
    'DEV_DB',                           -- Target database
    ARRAY_CONSTRUCT(                    -- Objects to migrate
        'dim_stocks',
        'fact_transactions',
        'vw_transaction_analysis'
    ),
    'IMCUST_TO_IMSDLC_SHARE',          -- Share name
    'IMSDLC'                           -- Target account
);
```

**Expected Output:**
```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                   SOURCE-SIDE MIGRATION ORCHESTRATION                         ║
╚═══════════════════════════════════════════════════════════════════════════════╝

🆔 MIGRATION ID: 3

📦 SOURCE CONFIGURATION:
   • Database: PROD_DB
   • Initial Schema: MART_INVESTMENTS_BOLT
   • Requested Objects: 3
   ...

✅ STATUS: Migration preparation completed successfully
```

### Test 2: Verify DDL Scripts (Views Only)

```sql
-- Check object types in DDL scripts
SELECT
    object_type,
    COUNT(*) as count
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = 3  -- Use your migration_id
GROUP BY object_type;

-- Expected: Only 'VIEW' type, NO 'TABLE' type
```

### Test 3: Verify Database References Fixed

```sql
-- Check view DDLs contain DEV_DB, not PROD_DB
SELECT
    object_name,
    CASE
        WHEN target_ddl LIKE '%DEV_DB%' THEN '✅ Correct'
        WHEN target_ddl LIKE '%PROD_DB%' THEN '❌ Wrong DB'
        ELSE '⚠️ Check manually'
    END as validation,
    target_ddl
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = 3
  AND object_type = 'VIEW';

-- Expected: All show '✅ Correct'
```

### Test 4: Target Side Migration (IMSDLC)

```sql
-- Connect to IMSDLC account
USE ROLE ACCOUNTADMIN;
USE DATABASE dev_db;
USE SCHEMA admin_schema;

-- Create shared database if needed
CREATE DATABASE IF NOT EXISTS IMCUST_SHARED_DB
FROM SHARE NFMYIZV.IMCUST.IMCUST_TO_IMSDLC_SHARE;

-- Execute full migration
CALL dev_db.admin_schema.sp_execute_full_migration(
    3,                      -- migration_id from source
    'IMCUST_SHARED_DB',     -- Shared database
    'ADMIN_SCHEMA',         -- Admin schema in shared DB
    'DEV_DB',              -- Target database
    'ADMIN_SCHEMA'         -- Admin schema for logs
);
```

**Expected Output:**
```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                   TARGET-SIDE MIGRATION EXECUTION                             ║
╚═══════════════════════════════════════════════════════════════════════════════╝

🔄 EXECUTION PLAN:
   Step 1: Execute CTAS scripts (create tables with data)
   Step 2: Execute DDL scripts (create views only)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                     STEP 1: CTAS EXECUTION (TABLES WITH DATA)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 EXECUTION SUMMARY:
   • Total Tables Migrated: 11
   • Successful: 11
   • Failed: 0
   ...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                          STEP 2: DDL EXECUTION (VIEWS ONLY)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 EXECUTION SUMMARY:
   • Total View DDLs Executed: 2
   • Successful: 2
   • Failed: 0
   ...
```

### Test 5: Verify Execution Order

```sql
-- Check that CTAS executed before DDL
SELECT
    log_id,
    execution_phase,
    object_name,
    status
FROM dev_db.admin_schema.migration_execution_log
WHERE migration_id = 3
ORDER BY log_id;

-- Expected: CTAS_EXECUTION entries come BEFORE DDL_EXECUTION entries
```

### Test 6: Test Views Work

```sql
-- Test views return data without errors
SELECT * FROM dev_db.MART_INVESTMENTS_BOLT.vw_transaction_analysis LIMIT 5;

-- Should see data with DEV_DB references working
```

### Test 7: Row Count Validation

```sql
-- Quick row count comparison
SELECT
    'dim_stocks' as table_name,
    (SELECT COUNT(*) FROM IMCUST_SHARED_DB.MART_INVESTMENTS_BOLT.dim_stocks) as shared_count,
    (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.dim_stocks) as target_count,
    CASE
        WHEN shared_count = target_count THEN '✅ MATCH'
        ELSE '❌ MISMATCH'
    END as validation;

-- Expected: ✅ MATCH
```

---

## 🎯 Success Criteria Checklist

After running all tests, verify:

- [ ] Source migration completes with detailed output
- [ ] Only VIEWs in migration_ddl_scripts (no TABLEs)
- [ ] View DDLs reference DEV_DB (not PROD_DB)
- [ ] Target migration completes successfully
- [ ] CTAS executes before DDL (check log_id order)
- [ ] All views query successfully
- [ ] Row counts match between shared and target
- [ ] No errors in migration_execution_log

---

## 📊 Quick Stats Queries

### Summary of Last Migration (IMCUST)

```sql
-- Get latest migration summary
SELECT
    migration_id,
    source_database,
    source_schema,
    target_database,
    status,
    created_ts
FROM PROD_DB.ADMIN_SCHEMA.migration_config
ORDER BY migration_id DESC
LIMIT 1;

-- Get object breakdown
SELECT
    source_schema,
    object_type,
    COUNT(*) as count
FROM PROD_DB.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = (SELECT MAX(migration_id) FROM PROD_DB.ADMIN_SCHEMA.migration_config)
GROUP BY source_schema, object_type
ORDER BY source_schema, object_type;
```

### Execution Results (IMSDLC)

```sql
-- Get execution summary
SELECT
    execution_phase,
    script_type,
    status,
    COUNT(*) as count,
    ROUND(AVG(execution_time_ms), 2) as avg_time_ms
FROM dev_db.admin_schema.migration_execution_log
WHERE migration_id = 3  -- Use your migration_id
GROUP BY execution_phase, script_type, status
ORDER BY execution_phase, script_type, status;
```

---

## 🔍 Troubleshooting

### Issue: View Creation Fails with "Object Doesn't Exist"

**Check if Fix #2 was applied:**
```sql
SELECT
    object_name,
    target_ddl
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = 3
  AND target_ddl LIKE '%PROD_DB%';

-- Should return NO rows (all should reference DEV_DB)
```

### Issue: Tables Created Twice

**Check if Fix #1 was applied:**
```sql
SELECT COUNT(*) as table_ddl_count
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = 3
  AND object_type = 'TABLE';

-- Should return 0 (no table DDLs)
```

### Issue: Views Created Before Tables

**Check if Fix #3 was applied:**
```sql
-- First DDL execution should have higher log_id than last CTAS
SELECT
    MIN(CASE WHEN execution_phase = 'DDL_EXECUTION' THEN log_id END) as first_ddl_log_id,
    MAX(CASE WHEN execution_phase = 'CTAS_EXECUTION' THEN log_id END) as last_ctas_log_id
FROM dev_db.admin_schema.migration_execution_log
WHERE migration_id = 3;

-- first_ddl_log_id should be > last_ctas_log_id
```

---

## 📞 Quick Commands Reference

```bash
# Deploy to both accounts
./deploy_all_fixes.sh

# View logs for source deployment
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)
snow sql -q "SHOW PROCEDURES LIKE 'sp_%';" -c imcust

# View logs for target deployment
export SNOWFLAKE_PASSWORD=$(cat .env.imsdlc_pat)
snow sql -q "SHOW PROCEDURES LIKE 'sp_%';" -c imsdlc

# Quick validation - check procedure versions
snow sql -q "SELECT SYSTEM\$VERSION();" -c imcust
snow sql -q "SELECT SYSTEM\$VERSION();" -c imsdlc
```

---

**Version:** 2.1
**Last Updated:** 2025-11-17
**Status:** Ready for Testing
