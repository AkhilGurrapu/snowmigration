# Updated Test Command - Multi-Level View Dependencies

## 🎯 What's New

The test dataset now includes **6 VIEWS** with a **multi-level dependency hierarchy** to properly test all three fixes:

### View Dependency Tree

```
📊 REQUESTED OBJECTS (what you migrate):
├─ vw_final_investment_dashboard (VIEW - Level 0)
│  ├─ vw_trading_summary (VIEW - Level 1) ← Discovered as dependency
│  │  └─ vw_transaction_analysis (VIEW - Level 2) ← Discovered as dependency
│  │     ├─ fact_transactions (TABLE)
│  │     ├─ dim_stocks (TABLE)
│  │     ├─ dim_brokers (TABLE)
│  │     ├─ daily_stock_performance (TABLE)
│  │     └─ stock_master (TABLE - SRC schema)
│  └─ vw_stock_performance_summary (VIEW - Level 1) ← Discovered as dependency
│     ├─ stock_master (TABLE - SRC schema)
│     └─ stock_prices_raw (TABLE - SRC schema)
│
└─ vw_portfolio_value_tracker (VIEW - Level 0)
   └─ vw_portfolio_performance (VIEW - Level 1) ← Discovered as dependency
      ├─ portfolio_summary (TABLE)
      ├─ customer_accounts (TABLE - SRC schema)
      ├─ dim_stocks (TABLE)
      ├─ dim_brokers (TABLE)
      └─ stock_prices_raw (TABLE - SRC schema)
```

### What This Tests

| Fix | Test Scenario |
|-----|---------------|
| **Fix #1** | NO table DDLs generated - only 6 view DDLs (all views in tree) |
| **Fix #2** | ALL view DDLs have PROD_DB → DEV_DB replacement at every level |
| **Fix #3** | Execution order: Tables → Level 2 views → Level 1 views → Level 0 views |

---

## 🚀 UPDATED TEST COMMAND

### Step 1: Recreate Test Dataset with New Views

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)
snow sql -f IMCUST/00_create_test_dataset.sql -c imcust
```

**This creates:**
- 11 tables (5 in MART, 6 in SRC)
- 6 views with multi-level dependencies (all with explicit PROD_DB references)

---

### Step 2: Run Migration with Top-Level Views

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat) && snow sql -q "CALL PROD_DB.ADMIN_SCHEMA.sp_orchestrate_migration(
'PROD_DB',
'MART_INVESTMENTS_BOLT',
'ADMIN_SCHEMA',
'DEV_DB',
ARRAY_CONSTRUCT('vw_final_investment_dashboard', 'vw_portfolio_value_tracker'),
'IMCUST_TO_IMSDLC_SHARE',
'IMSDLC'
);" -c imcust
```

**Key Changes:**
- ✅ Request only **2 top-level views**
- ✅ GET_LINEAGE discovers **4 intermediate views** automatically
- ✅ GET_LINEAGE discovers **11 tables** across both schemas
- ✅ Total: **2 requested + 4 view dependencies + 11 table dependencies = 17 objects**

---

### Expected Output

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                   SOURCE-SIDE MIGRATION ORCHESTRATION                         ║
╚═══════════════════════════════════════════════════════════════════════════════╝

🆔 MIGRATION ID: 4

📦 SOURCE CONFIGURATION:
   • Database: PROD_DB
   • Initial Schema: MART_INVESTMENTS_BOLT
   • Admin Schema: ADMIN_SCHEMA
   • Requested Objects: 2

🎯 TARGET CONFIGURATION:
   • Database: DEV_DB
   • Account: IMSDLC
   • Share Name: IMCUST_TO_IMSDLC_SHARE

Found 17 total objects (including 2 requested objects and 15 dependencies) across 3 levels

📂 OBJECT BREAKDOWN BY SCHEMA:
   • MART_INVESTMENTS_BOLT.TABLE: 5
   • MART_INVESTMENTS_BOLT.VIEW: 5
   • SRC_INVESTMENTS_BOLT.TABLE: 6
   • SRC_INVESTMENTS_BOLT.VIEW: 1

📊 OBJECTS PROCESSED:
   • Total Objects: 17
   • Tables: 11
   • Views: 6

📝 SCRIPTS GENERATED:
   • View DDL Scripts: 6 (for views only - tables use CTAS)
   • CTAS Scripts: 11 (for data migration)

✅ RESULT: 6 view DDLs + 11 CTAS scripts ready for migration

✅ STATUS: Migration preparation completed successfully
```

---

## ✅ Critical Validations

### Validation 1: Verify Only View DDLs Generated (Fix #1)

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)

snow sql -q "
SELECT
    object_type,
    COUNT(*) as count
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = 4  -- Use your migration_id
GROUP BY object_type;
" -c imcust
```

**Expected:**
```
OBJECT_TYPE | COUNT
VIEW        | 6
(NO TABLE entries)
```

### Validation 2: Verify All View Dependencies Discovered

```bash
snow sql -q "
SELECT
    object_name,
    object_type,
    dependency_level,
    source_schema
FROM PROD_DB.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = 4
  AND object_type = 'VIEW'
ORDER BY dependency_level DESC, object_name;
" -c imcust
```

**Expected Views:**
```
OBJECT_NAME                          | TYPE | LEVEL | SCHEMA
vw_transaction_analysis              | VIEW | 2     | MART_INVESTMENTS_BOLT
vw_stock_performance_summary         | VIEW | 1     | MART_INVESTMENTS_BOLT
vw_portfolio_performance             | VIEW | 1     | MART_INVESTMENTS_BOLT
vw_trading_summary                   | VIEW | 1     | MART_INVESTMENTS_BOLT
vw_final_investment_dashboard        | VIEW | 0     | MART_INVESTMENTS_BOLT
vw_portfolio_value_tracker           | VIEW | 0     | MART_INVESTMENTS_BOLT
```

### Validation 3: Verify Database References Replaced (Fix #2)

```bash
snow sql -q "
SELECT
    object_name,
    CASE
        WHEN target_ddl LIKE '%DEV_DB%' AND target_ddl NOT LIKE '%PROD_DB%'
        THEN '✅ CORRECT'
        WHEN target_ddl LIKE '%PROD_DB%'
        THEN '❌ WRONG - Still has PROD_DB'
        ELSE '⚠️ CHECK'
    END as validation,
    LENGTH(target_ddl) as ddl_length
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = 4
  AND object_type = 'VIEW'
ORDER BY object_name;
" -c imcust
```

**Expected:** All 6 views show `✅ CORRECT`

### Validation 4: View One Specific DDL (Detailed Check)

```bash
snow sql -q "
SELECT target_ddl
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = 4
  AND object_name = 'vw_final_investment_dashboard';
" -c imcust
```

**Verify it contains:**
- ✅ `DEV_DB.MART_INVESTMENTS_BOLT.vw_trading_summary`
- ✅ `DEV_DB.MART_INVESTMENTS_BOLT.vw_stock_performance_summary`
- ❌ NO `PROD_DB` anywhere

---

## 🎯 Target-Side Testing

### Step 3: Execute Migration on Target

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imsdlc_pat)

# Create shared database (if needed)
snow sql -q "CREATE DATABASE IF NOT EXISTS IMCUST_SHARED_DB
             FROM SHARE NFMYIZV.IMCUST.IMCUST_TO_IMSDLC_SHARE;" -c imsdlc

# Execute migration
snow sql -q "CALL dev_db.admin_schema.sp_execute_full_migration(
    4,  -- migration_id from source
    'IMCUST_SHARED_DB',
    'ADMIN_SCHEMA',
    'DEV_DB',
    'ADMIN_SCHEMA'
);" -c imsdlc
```

**Expected Output:**
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                     STEP 1: CTAS EXECUTION (TABLES WITH DATA)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 EXECUTION SUMMARY:
   • Total Tables Migrated: 11
   • Successful: 11
   • Failed: 0

✅ SUCCESSFULLY CREATED TABLES:
   • MART_INVESTMENTS_BOLT.dim_stocks (TABLE)
   • MART_INVESTMENTS_BOLT.dim_brokers (TABLE)
   • MART_INVESTMENTS_BOLT.fact_transactions (TABLE)
   • MART_INVESTMENTS_BOLT.daily_stock_performance (TABLE)
   • MART_INVESTMENTS_BOLT.portfolio_summary (TABLE)
   • SRC_INVESTMENTS_BOLT.stock_master (TABLE)
   • SRC_INVESTMENTS_BOLT.stock_prices_raw (TABLE)
   • SRC_INVESTMENTS_BOLT.transactions_raw (TABLE)
   • SRC_INVESTMENTS_BOLT.broker_master (TABLE)
   • SRC_INVESTMENTS_BOLT.customer_accounts (TABLE)
   • SRC_INVESTMENTS_BOLT.account_balance_history (TABLE)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                          STEP 2: DDL EXECUTION (VIEWS ONLY)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 EXECUTION SUMMARY:
   • Total View DDLs Executed: 6
   • Successful: 6
   • Failed: 0

✅ SUCCESSFULLY CREATED VIEWS:
   • vw_transaction_analysis (VIEW)
   • vw_stock_performance_summary (VIEW)
   • vw_portfolio_performance (VIEW)
   • vw_trading_summary (VIEW)
   • vw_final_investment_dashboard (VIEW)
   • vw_portfolio_value_tracker (VIEW)
```

### Validation 5: Verify Execution Order (Fix #3)

```bash
snow sql -q "
SELECT
    log_id,
    execution_phase,
    object_name,
    script_type,
    status
FROM dev_db.admin_schema.migration_execution_log
WHERE migration_id = 4
ORDER BY log_id;
" -c imsdlc
```

**Expected Order:**
1. **CTAS_EXECUTION** for all 11 tables (log_id 1-11)
2. **DDL_EXECUTION** for all 6 views (log_id 12-17)

**Key Check:** All CTAS log_ids < All DDL log_ids ✅

### Validation 6: Test All Views Work

```bash
# Test top-level view 1
snow sql -q "
SELECT * FROM dev_db.MART_INVESTMENTS_BOLT.vw_final_investment_dashboard LIMIT 3;
" -c imsdlc

# Test top-level view 2
snow sql -q "
SELECT * FROM dev_db.MART_INVESTMENTS_BOLT.vw_portfolio_value_tracker LIMIT 3;
" -c imsdlc

# Test intermediate view
snow sql -q "
SELECT * FROM dev_db.MART_INVESTMENTS_BOLT.vw_trading_summary LIMIT 3;
" -c imsdlc
```

**Expected:** All queries return data successfully (proves DEV_DB references work at all levels)

---

## 📊 Complete View Hierarchy Validation

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imsdlc_pat)

snow sql -q "
-- Show view dependency relationships
SELECT
    'vw_final_investment_dashboard' as view_name,
    'depends on' as relationship,
    'vw_trading_summary + vw_stock_performance_summary' as dependencies
UNION ALL
SELECT
    'vw_trading_summary',
    'depends on',
    'vw_transaction_analysis'
UNION ALL
SELECT
    'vw_transaction_analysis',
    'depends on',
    'tables: fact_transactions, dim_stocks, etc.'
UNION ALL
SELECT
    'vw_portfolio_value_tracker',
    'depends on',
    'vw_portfolio_performance'
UNION ALL
SELECT
    'vw_portfolio_performance',
    'depends on',
    'tables: portfolio_summary, customer_accounts, etc.';
" -c imsdlc
```

---

## 🎯 Summary of Test Coverage

| Test Aspect | Coverage |
|-------------|----------|
| **Objects Requested** | 2 views |
| **Objects Discovered** | 15 (4 views + 11 tables) |
| **Total Migrated** | 17 objects |
| **View Dependency Levels** | 3 levels (0, 1, 2) |
| **Cross-Schema References** | ✅ SRC ↔ MART |
| **View-on-View Dependencies** | ✅ 4 layers |
| **Database Reference Replacement** | ✅ All 6 views |
| **Table DDL Generation** | ✅ Skipped (0 generated) |
| **Execution Order** | ✅ Tables → Views (by level) |

---

## 🚀 Automated Test Script

```bash
# Run complete automated test
./test_migration_fixes.sh
```

Or update the script with new objects:

```bash
# Edit test_migration_fixes.sh and change line:
ARRAY_CONSTRUCT('dim_stocks', 'fact_transactions', 'vw_transaction_analysis')

# To:
ARRAY_CONSTRUCT('vw_final_investment_dashboard', 'vw_portfolio_value_tracker')
```

---

## ✅ Success Criteria

After running the migration:

- [ ] **17 total objects** discovered (2 requested + 15 dependencies)
- [ ] **6 view DDLs** generated (NO table DDLs)
- [ ] **11 CTAS scripts** generated (all tables)
- [ ] **All view DDLs** contain DEV_DB references only
- [ ] **Execution order:** CTAS (tables) → DDL (views by dependency level)
- [ ] **All 6 views** query successfully on target
- [ ] **Row counts** match between shared and target

---

**Version:** 2.1 Enhanced
**Test Dataset:** Multi-level view dependencies with explicit PROD_DB references
**Last Updated:** 2025-11-17
