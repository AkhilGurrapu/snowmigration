# Migration Framework v2.1 - Test Commands

## 🚀 Quick Test (Your Command)

### Option 1: Automated Full Test Suite (Recommended)

```bash
cd /Users/akhilgurrapu/Downloads/snowmigration
./test_migration_fixes.sh
```

**This will:**
1. Deploy all fixes to both accounts
2. Recreate test dataset with updated views
3. Run source-side migration
4. Run target-side migration
5. Validate all 3 fixes automatically
6. Show detailed results

---

### Option 2: Manual Testing (Your Original Command)

#### Step 1: Recreate Test Dataset First

```bash
# Update test data with new view definitions
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)
snow sql -f IMCUST/00_create_test_dataset.sql -c imcust
```

**What changed in the view:**
- `vw_transaction_analysis` now has **explicit PROD_DB references**
- Tests both same-schema (MART) and cross-schema (SRC) references
- Perfect for validating Fix #2 (database reference replacement)

**New view structure:**
```sql
FROM PROD_DB.MART_INVESTMENTS_BOLT.fact_transactions ft
JOIN PROD_DB.MART_INVESTMENTS_BOLT.dim_stocks ds ...
-- Cross-schema reference to test Fix #2
LEFT JOIN PROD_DB.SRC_INVESTMENTS_BOLT.stock_master sm ...
```

#### Step 2: Run Source-Side Migration

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat) && snow sql -q "CALL PROD_DB.ADMIN_SCHEMA.sp_orchestrate_migration(
'PROD_DB',
'MART_INVESTMENTS_BOLT',
'ADMIN_SCHEMA',
'DEV_DB',
ARRAY_CONSTRUCT('dim_stocks', 'fact_transactions', 'vw_transaction_analysis'),
'IMCUST_TO_IMSDLC_SHARE',
'IMSDLC'
);" -c imcust
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

📊 OBJECTS PROCESSED:
   • Total Objects: 8 (7 tables, 1 view)
   • Tables: 7
   • Views: 1

📝 SCRIPTS GENERATED:
   • View DDL Scripts: 1 (for views only - tables use CTAS)
   • CTAS Scripts: 7 (for data migration)

📂 OBJECT BREAKDOWN BY SCHEMA:
   • MART_INVESTMENTS_BOLT.TABLE: 5
   • MART_INVESTMENTS_BOLT.VIEW: 1
   • SRC_INVESTMENTS_BOLT.TABLE: 2

✅ STATUS: Migration preparation completed successfully
```

#### Step 3: Validate Source-Side (Critical!)

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)

# Get the migration_id from previous output
MIGRATION_ID=3  # Replace with actual ID

# Validation 1: Verify NO table DDLs (Fix #1)
snow sql -q "
SELECT object_type, COUNT(*) as count
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = $MIGRATION_ID
GROUP BY object_type;
" -c imcust
# Expected: Only VIEW type, count = 1
```

```bash
# Validation 2: Verify view DDL has DEV_DB references (Fix #2)
snow sql -q "
SELECT
    object_name,
    target_ddl
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = $MIGRATION_ID
  AND object_type = 'VIEW';
" -c imcust
# Expected: target_ddl contains DEV_DB.MART_INVESTMENTS_BOLT and DEV_DB.SRC_INVESTMENTS_BOLT
# Expected: target_ddl does NOT contain PROD_DB anywhere
```

**Check manually that the DDL contains:**
- `DEV_DB.MART_INVESTMENTS_BOLT.fact_transactions` ✅
- `DEV_DB.MART_INVESTMENTS_BOLT.dim_stocks` ✅
- `DEV_DB.SRC_INVESTMENTS_BOLT.stock_master` ✅
- NO `PROD_DB` references ✅

#### Step 4: Run Target-Side Migration

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imsdlc_pat)

# Create shared database (if needed)
snow sql -q "CREATE DATABASE IF NOT EXISTS IMCUST_SHARED_DB
             FROM SHARE NFMYIZV.IMCUST.IMCUST_TO_IMSDLC_SHARE;" -c imsdlc

# Execute migration
snow sql -q "CALL dev_db.admin_schema.sp_execute_full_migration(
    $MIGRATION_ID,
    'IMCUST_SHARED_DB',
    'ADMIN_SCHEMA',
    'DEV_DB',
    'ADMIN_SCHEMA'
);" -c imsdlc
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
   • Total Tables Migrated: 7
   • Successful: 7
   • Failed: 0

✅ SUCCESSFULLY CREATED TABLES:
   • MART_INVESTMENTS_BOLT.dim_stocks (TABLE)
   • MART_INVESTMENTS_BOLT.dim_brokers (TABLE)
   • MART_INVESTMENTS_BOLT.fact_transactions (TABLE)
   • MART_INVESTMENTS_BOLT.daily_stock_performance (TABLE)
   • MART_INVESTMENTS_BOLT.portfolio_summary (TABLE)
   • SRC_INVESTMENTS_BOLT.stock_master (TABLE)
   • SRC_INVESTMENTS_BOLT.stock_prices_raw (TABLE)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                          STEP 2: DDL EXECUTION (VIEWS ONLY)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 EXECUTION SUMMARY:
   • Total View DDLs Executed: 1
   • Successful: 1
   • Failed: 0

✅ SUCCESSFULLY CREATED VIEWS:
   • vw_transaction_analysis (VIEW)
```

#### Step 5: Validate Target-Side

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imsdlc_pat)

# Validation 3: Verify execution order (Fix #3)
snow sql -q "
SELECT
    execution_phase,
    MIN(log_id) as first_log_id,
    MAX(log_id) as last_log_id
FROM dev_db.admin_schema.migration_execution_log
WHERE migration_id = $MIGRATION_ID
GROUP BY execution_phase
ORDER BY first_log_id;
" -c imsdlc
# Expected: CTAS_EXECUTION has lower first_log_id than DDL_EXECUTION
```

```bash
# Validation 4: Test view actually works (Fix #2 validated!)
snow sql -q "
SELECT * FROM dev_db.MART_INVESTMENTS_BOLT.vw_transaction_analysis LIMIT 5;
" -c imsdlc
# Expected: Returns 5 rows successfully (proves DEV_DB references work)
```

---

## 🎯 What Each Object Tests

| Object | Type | Tests |
|--------|------|-------|
| `dim_stocks` | TABLE | Fix #1 (no DDL, CTAS only) |
| `fact_transactions` | TABLE | Fix #1 (no DDL, CTAS only) |
| `vw_transaction_analysis` | VIEW | Fix #2 (PROD_DB → DEV_DB replacement)<br>Fix #3 (view created after tables via CTAS) |

**Dependencies Discovered by GET_LINEAGE:**
```
vw_transaction_analysis (VIEW)
├── fact_transactions (TABLE) - same schema
├── dim_stocks (TABLE) - same schema
├── dim_brokers (TABLE) - same schema
├── daily_stock_performance (TABLE) - same schema
├── portfolio_summary (TABLE) - same schema
└── stock_master (TABLE) - CROSS-SCHEMA (SRC_INVESTMENTS_BOLT)
```

---

## ✅ Success Criteria

After running the test, verify:

### Fix #1: No Table DDLs
```bash
# Should return 0 table DDLs
SELECT COUNT(*) FROM migration_ddl_scripts
WHERE migration_id = X AND object_type = 'TABLE';
-- Expected: 0
```

### Fix #2: Database References Replaced
```bash
# View DDL should have DEV_DB, not PROD_DB
SELECT target_ddl FROM migration_ddl_scripts
WHERE migration_id = X AND object_type = 'VIEW';
-- Should contain: DEV_DB.MART_INVESTMENTS_BOLT
-- Should contain: DEV_DB.SRC_INVESTMENTS_BOLT
-- Should NOT contain: PROD_DB
```

### Fix #3: Correct Execution Order
```bash
# CTAS log_ids should be lower than DDL log_ids
SELECT execution_phase, MIN(log_id), MAX(log_id)
FROM migration_execution_log
WHERE migration_id = X
GROUP BY execution_phase;
-- CTAS_EXECUTION min/max should be < DDL_EXECUTION min/max
```

---

## 🔍 View the Updated View Definition

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)
snow sql -q "SHOW CREATE VIEW PROD_DB.MART_INVESTMENTS_BOLT.vw_transaction_analysis;" -c imcust
```

**Key Changes:**
- ✅ Explicit `PROD_DB.MART_INVESTMENTS_BOLT.*` references (will become DEV_DB)
- ✅ Cross-schema join to `PROD_DB.SRC_INVESTMENTS_BOLT.stock_master` (will become DEV_DB)
- ✅ Tests both same-schema and cross-schema database reference replacement

---

## 📊 Quick Validation Queries

```sql
-- Source Side (IMCUST)
-- ===================

-- 1. Check migration config
SELECT * FROM PROD_DB.ADMIN_SCHEMA.migration_config
ORDER BY migration_id DESC LIMIT 1;

-- 2. Check discovered objects
SELECT source_schema, object_type, object_name, dependency_level
FROM PROD_DB.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = X
ORDER BY dependency_level DESC, source_schema, object_name;

-- 3. Check DDL scripts (should be views only)
SELECT object_type, object_name,
       CASE WHEN target_ddl LIKE '%DEV_DB%' THEN '✅' ELSE '❌' END as has_dev_db,
       CASE WHEN target_ddl LIKE '%PROD_DB%' THEN '❌' ELSE '✅' END as no_prod_db
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = X;

-- 4. Check CTAS scripts (should be tables only)
SELECT COUNT(*) as ctas_count
FROM PROD_DB.ADMIN_SCHEMA.migration_ctas_scripts
WHERE migration_id = X;


-- Target Side (IMSDLC)
-- ====================

-- 5. Check execution log
SELECT execution_phase, script_type, object_name, status, log_id
FROM dev_db.admin_schema.migration_execution_log
WHERE migration_id = X
ORDER BY log_id;

-- 6. Verify execution order
SELECT
    execution_phase,
    MIN(log_id) as first_execution,
    MAX(log_id) as last_execution,
    COUNT(*) as count
FROM dev_db.admin_schema.migration_execution_log
WHERE migration_id = X
GROUP BY execution_phase
ORDER BY first_execution;

-- 7. Test view queries
SELECT * FROM dev_db.MART_INVESTMENTS_BOLT.vw_transaction_analysis LIMIT 5;

-- 8. Row count validation
SELECT
    'fact_transactions' as table_name,
    (SELECT COUNT(*) FROM IMCUST_SHARED_DB.MART_INVESTMENTS_BOLT.fact_transactions) as source,
    (SELECT COUNT(*) FROM dev_db.MART_INVESTMENTS_BOLT.fact_transactions) as target;
```

---

## 🚀 Recommended Workflow

1. **First time:**
   ```bash
   ./test_migration_fixes.sh  # Full automated test
   ```

2. **For manual control:**
   - Recreate test dataset
   - Run your command (source-side migration)
   - Validate with queries above
   - Run target-side migration
   - Validate results

3. **For subsequent tests:**
   - Just run source → target migration
   - Validation queries to confirm

---

**Version:** 2.1
**Updated:** 2025-11-17 with explicit PROD_DB references in views
