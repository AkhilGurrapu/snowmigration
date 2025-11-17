# Test Unqualified Reference Fix (Fix #2.1)

## 🎯 New Test Dataset Structure

The test dataset now includes **3 SRC VIEWS** with **unqualified table references** to properly test Fix #2.1.

### Complete Dependency Tree

```
📊 REQUESTED OBJECTS:
├─ vw_transaction_analysis (VIEW - MART)
│  │
│  ├─ Layer 2: MART Intermediate Views
│  │  ├─ vw_enriched_transactions (VIEW - MART)
│  │  │  └─ fact_transactions (TABLE - MART)
│  │  │
│  │  ├─ vw_stock_dimensions (VIEW - MART)
│  │  │  ├─ dim_stocks (TABLE - MART)
│  │  │  └─ vw_stock_master_enhanced (VIEW - SRC) ⭐ NEW!
│  │  │     ├─ stock_master (TABLE - SRC) ❌ Unqualified ref
│  │  │     └─ stock_prices_raw (TABLE - SRC) ❌ Unqualified ref
│  │  │
│  │  ├─ vw_broker_info (VIEW - MART)
│  │  │  ├─ dim_brokers (TABLE - MART)
│  │  │  └─ vw_broker_master_enhanced (VIEW - SRC) ⭐ NEW!
│  │  │     ├─ broker_master (TABLE - SRC) ❌ Unqualified ref
│  │  │     └─ customer_accounts (TABLE - SRC) ❌ Unqualified ref
│  │  │
│  │  └─ vw_daily_performance (VIEW - MART)
│  │     └─ daily_stock_performance (TABLE - MART)
│  │
│  └─ Layer 1: SRC Views (Discovered by GET_LINEAGE)
│     └─ 3 SRC VIEWS with unqualified references
│
├─ fact_transactions (TABLE - requested)
└─ dim_stocks (TABLE - requested)
```

---

## 🔍 What Gets Tested

### **3 NEW SRC VIEWS (with unqualified refs):**

**1. vw_stock_master_enhanced (SRC schema)**
```sql
CREATE VIEW vw_stock_master_enhanced AS
SELECT sm.stock_id, sm.ticker, ...
FROM stock_master sm                -- ❌ UNQUALIFIED (same schema)
LEFT JOIN stock_prices_raw spr      -- ❌ UNQUALIFIED (same schema)
    ON sm.stock_id = spr.stock_id
```

**2. vw_broker_master_enhanced (SRC schema)**
```sql
CREATE VIEW vw_broker_master_enhanced AS
SELECT bm.broker_id, bm.broker_name, ...
FROM broker_master bm               -- ❌ UNQUALIFIED (same schema)
LEFT JOIN customer_accounts ca      -- ❌ UNQUALIFIED (same schema)
    ON bm.broker_id = ca.broker_id
```

**3. vw_transaction_metrics (SRC schema)**
```sql
CREATE VIEW vw_transaction_metrics AS
SELECT tr.transaction_id, ...
FROM transactions_raw tr            -- ❌ UNQUALIFIED (same schema)
JOIN stock_master sm                -- ❌ UNQUALIFIED (same schema)
    ON tr.stock_id = sm.stock_id
```

---

## ✅ Expected Behavior (Fix #2.1)

### **WITHOUT Fix #2.1:**
```sql
-- DDL would contain unqualified references:
CREATE VIEW DEV_DB.SRC_INVESTMENTS_BOLT.vw_stock_master_enhanced AS
SELECT sm.stock_id, sm.ticker, ...
FROM stock_master sm                -- ❌ ERROR: object doesn't exist on target!
LEFT JOIN stock_prices_raw spr      -- ❌ ERROR: object doesn't exist on target!
```

### **WITH Fix #2.1:**
```sql
-- DDL has fully qualified references:
CREATE VIEW DEV_DB.SRC_INVESTMENTS_BOLT.vw_stock_master_enhanced AS
SELECT sm.stock_id, sm.ticker, ...
FROM DEV_DB.SRC_INVESTMENTS_BOLT.stock_master sm      -- ✅ Fully qualified!
LEFT JOIN DEV_DB.SRC_INVESTMENTS_BOLT.stock_prices_raw spr  -- ✅ Fully qualified!
```

---

## 🚀 TEST COMMANDS

### Step 1: Deploy Fix #2.1

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)
snow sql -f IMCUST/03_sp_generate_migration_scripts.sql -c imcust
```

### Step 2: Recreate Test Dataset (with new SRC views)

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)
snow sql -f IMCUST/00_create_test_dataset.sql -c imcust
```

### Step 3: Run Your Migration Command

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

---

## 📊 Expected Discovery Results

### Objects Discovered by GET_LINEAGE:

| Object | Type | Schema | Level | Why Discovered |
|--------|------|--------|-------|----------------|
| **vw_transaction_analysis** | VIEW | MART | 0 | Requested |
| **fact_transactions** | TABLE | MART | 0 | Requested |
| **dim_stocks** | TABLE | MART | 0 | Requested |
| **vw_enriched_transactions** | VIEW | MART | 1 | Upstream of vw_transaction_analysis |
| **vw_stock_dimensions** | VIEW | MART | 1 | Upstream of vw_transaction_analysis |
| **vw_broker_info** | VIEW | MART | 1 | Upstream of vw_transaction_analysis |
| **vw_daily_performance** | VIEW | MART | 1 | Upstream of vw_transaction_analysis |
| **vw_stock_master_enhanced** | VIEW | SRC | 2 | Upstream of vw_stock_dimensions ⭐ |
| **vw_broker_master_enhanced** | VIEW | SRC | 2 | Upstream of vw_broker_info ⭐ |
| dim_brokers | TABLE | MART | 2 | Upstream of vw_broker_info |
| daily_stock_performance | TABLE | MART | 2 | Upstream of vw_daily_performance |
| stock_master | TABLE | SRC | 3 | Upstream of vw_stock_master_enhanced |
| stock_prices_raw | TABLE | SRC | 3 | Upstream of vw_stock_master_enhanced |
| broker_master | TABLE | SRC | 3 | Upstream of vw_broker_master_enhanced |
| customer_accounts | TABLE | SRC | 3 | Upstream of vw_broker_master_enhanced |

**Total:** 3 requested + 12 dependencies = **15 objects**
- **7 VIEWS** (1 requested + 6 discovered, including 3 SRC views)
- **8 TABLES** (2 requested + 6 discovered)

---

## ✅ Validation Queries

### Check SRC Views Discovered

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)

snow sql -q "
SELECT
    object_name,
    object_type,
    source_schema,
    dependency_level
FROM PROD_DB.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = <your_migration_id>
  AND source_schema = 'SRC_INVESTMENTS_BOLT'
  AND object_type = 'VIEW'
ORDER BY dependency_level DESC, object_name;
" -c imcust
```

**Expected: 3 SRC views**
```
OBJECT_NAME                    | TYPE | SCHEMA                | LEVEL
vw_stock_master_enhanced       | VIEW | SRC_INVESTMENTS_BOLT  | 2
vw_broker_master_enhanced      | VIEW | SRC_INVESTMENTS_BOLT  | 2
vw_transaction_metrics         | VIEW | SRC_INVESTMENTS_BOLT  | 2
```

### Check SRC View DDLs Have Fully Qualified References

```bash
snow sql -q "
SELECT
    object_name,
    target_ddl
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = <your_migration_id>
  AND object_name = 'vw_stock_master_enhanced';
" -c imcust
```

**Verify target_ddl contains:**
- ✅ `FROM DEV_DB.SRC_INVESTMENTS_BOLT.stock_master`
- ✅ `JOIN DEV_DB.SRC_INVESTMENTS_BOLT.stock_prices_raw`
- ❌ NO `FROM stock_master` (unqualified)
- ❌ NO `JOIN stock_prices_raw` (unqualified)

### Check All View DDLs are Fully Qualified

```bash
snow sql -q "
SELECT
    object_name,
    source_schema,
    CASE
        WHEN target_ddl LIKE '% stock_master %'
         AND target_ddl NOT LIKE '%SRC_INVESTMENTS_BOLT.stock_master%'
        THEN '❌ Has unqualified stock_master'
        WHEN target_ddl LIKE '% broker_master %'
         AND target_ddl NOT LIKE '%SRC_INVESTMENTS_BOLT.broker_master%'
        THEN '❌ Has unqualified broker_master'
        ELSE '✅ All refs qualified'
    END as validation
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = <your_migration_id>
  AND object_type = 'VIEW'
ORDER BY source_schema, object_name;
" -c imcust
```

**Expected:** All views show `✅ All refs qualified`

---

## 🎯 What This Proves

| Test Scenario | Coverage |
|---------------|----------|
| **SRC views discovered** | ✅ 3 SRC views as upstream deps |
| **Unqualified refs in same schema** | ✅ All SRC views have unqualified refs |
| **Fix #2.1 qualifies refs** | ✅ All unqualified → fully qualified |
| **Cross-schema view deps** | ✅ MART views → SRC views |
| **Multi-level view hierarchy** | ✅ 3 dependency levels (0, 1, 2) |
| **View-on-view-on-table** | ✅ Complete chain tested |

---

## 🚀 Target Side Test

After source-side migration completes:

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imsdlc_pat)

# Create shared database
snow sql -q "CREATE DATABASE IF NOT EXISTS IMCUST_SHARED_DB
             FROM SHARE NFMYIZV.IMCUST.IMCUST_TO_IMSDLC_SHARE;" -c imsdlc

# Execute migration
snow sql -q "CALL dev_db.admin_schema.sp_execute_full_migration(
    <migration_id>,
    'IMCUST_SHARED_DB',
    'ADMIN_SCHEMA',
    'DEV_DB',
    'ADMIN_SCHEMA'
);" -c imsdlc
```

**Expected:**
```
━━━ STEP 1: CTAS EXECUTION (TABLES WITH DATA) ━━━
📊 Tables Migrated: 8 | ✅ Success: 8 | ❌ Failed: 0

━━━ STEP 2: DDL EXECUTION (VIEWS ONLY) ━━━
📊 Views Executed: 7 | ✅ Success: 7 | ❌ Failed: 0
```

### Verify SRC Views Work on Target

```bash
# Test SRC view
snow sql -q "
SELECT * FROM dev_db.SRC_INVESTMENTS_BOLT.vw_stock_master_enhanced LIMIT 3;
" -c imsdlc

# Test MART view that uses SRC view
snow sql -q "
SELECT * FROM dev_db.MART_INVESTMENTS_BOLT.vw_stock_dimensions LIMIT 3;
" -c imsdlc

# Test final view
snow sql -q "
SELECT * FROM dev_db.MART_INVESTMENTS_BOLT.vw_transaction_analysis LIMIT 3;
" -c imsdlc
```

**Expected:** All queries return data successfully! ✅

---

## 📋 Success Criteria

- [ ] 3 SRC views discovered as upstream dependencies
- [ ] All SRC view DDLs have fully qualified table references
- [ ] No unqualified references like `FROM table_name` in any DDL
- [ ] All views created successfully on target
- [ ] All views query successfully on target
- [ ] Complete dependency chain works: final views → intermediate views → SRC views → tables

---

**Version:** 2.1 - Unqualified Reference Fix Test
**Updated:** 2025-11-17
**Test File:** IMCUST/00_create_test_dataset.sql
