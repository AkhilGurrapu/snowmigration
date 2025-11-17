# Final Test Command - View-on-View Dependencies

## 🎯 Updated Test Dataset Structure

### **NEW: Multi-Layer View Architecture**

```
📊 LAYER 2 (Final Views - What You Request):
└─ vw_transaction_analysis
   │
   ├─ 📊 LAYER 1 (Intermediate Views - Discovered by GET_LINEAGE):
   │  ├─ vw_enriched_transactions (VIEW)
   │  ├─ vw_stock_dimensions (VIEW)
   │  ├─ vw_broker_info (VIEW)
   │  └─ vw_daily_performance (VIEW)
   │     │
   │     └─ 📦 LAYER 0 (Base Tables):
   │        ├─ fact_transactions (TABLE)
   │        ├─ dim_stocks (TABLE)
   │        ├─ dim_brokers (TABLE)
   │        ├─ daily_stock_performance (TABLE)
   │        ├─ stock_master (TABLE - SRC schema)
   │        └─ broker_master (TABLE - SRC schema)
```

### **What Changed:**

**BEFORE:**
- `vw_transaction_analysis` directly queried tables
- GET_LINEAGE discovered only TABLES as dependencies
- ❌ No view-on-view testing

**AFTER:**
- `vw_transaction_analysis` queries 4 INTERMEDIATE VIEWS
- GET_LINEAGE discovers **4 VIEWS** as upstream dependencies
- ✅ Proper view-on-view dependency testing
- ✅ All views have explicit PROD_DB references

---

## 🚀 YOUR TEST COMMAND (No Changes Needed!)

### Step 1: Recreate Test Dataset

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)
snow sql -f IMCUST/00_create_test_dataset.sql -c imcust
```

### Step 2: Run Your Original Command

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

## ✅ Expected Results

### **Objects Discovered:**

| Object | Type | Level | Discovered As |
|--------|------|-------|---------------|
| `vw_transaction_analysis` | VIEW | 0 | Requested |
| `vw_enriched_transactions` | VIEW | 1 | **Upstream Dependency** ⭐ |
| `vw_stock_dimensions` | VIEW | 1 | **Upstream Dependency** ⭐ |
| `vw_broker_info` | VIEW | 1 | **Upstream Dependency** ⭐ |
| `vw_daily_performance` | VIEW | 1 | **Upstream Dependency** ⭐ |
| `fact_transactions` | TABLE | 0 | Requested |
| `dim_stocks` | TABLE | 0 | Requested |
| `dim_brokers` | TABLE | 2 | Upstream Dependency |
| `daily_stock_performance` | TABLE | 2 | Upstream Dependency |
| `stock_master` | TABLE | 2 | Cross-schema Dependency |
| `broker_master` | TABLE | 2 | Cross-schema Dependency |

**Total:** 3 requested + 8 dependencies = **11 objects**
- **5 VIEWS** (1 requested + 4 intermediate discovered)
- **6 TABLES** (2 requested + 4 discovered)

### **Expected Output:**

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                   SOURCE-SIDE MIGRATION ORCHESTRATION                         ║
╚═══════════════════════════════════════════════════════════════════════════════╝

🆔 MIGRATION ID: X

📦 SOURCE CONFIGURATION:
   • Requested Objects: 3

Found 11 total objects (including 3 requested objects and 8 dependencies) across 3 levels

📂 OBJECT BREAKDOWN BY SCHEMA:
   • MART_INVESTMENTS_BOLT.TABLE: 4
   • MART_INVESTMENTS_BOLT.VIEW: 5   ← ⭐ 4 intermediate views discovered!
   • SRC_INVESTMENTS_BOLT.TABLE: 2

📝 SCRIPTS GENERATED:
   • View DDL Scripts: 5 (for views only - tables use CTAS)
   • CTAS Scripts: 6 (for data migration)
```

---

## 🎯 Key Testing Points

### 1. **View-on-View Dependencies** ✅
- Request: `vw_transaction_analysis`
- Discovers: 4 intermediate VIEWS as dependencies
- Those views depend on tables

### 2. **Database Reference Replacement (Fix #2)** ✅
- All 5 view DDLs have PROD_DB references
- Must be replaced with DEV_DB
- Tests replacement at multiple dependency levels

### 3. **No Table DDLs (Fix #1)** ✅
- 6 tables discovered
- 0 table DDLs generated
- Only 5 view DDLs generated

### 4. **Correct Execution Order (Fix #3)** ✅
- Step 1: CTAS creates 6 tables
- Step 2: DDL creates 5 views (in dependency order)

---

## 📋 Validation Queries

### Check Intermediate Views Discovered

```bash
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)

snow sql -q "
SELECT
    object_name,
    object_type,
    dependency_level
FROM PROD_DB.ADMIN_SCHEMA.migration_share_objects
WHERE migration_id = X  -- Use your migration_id
  AND object_type = 'VIEW'
ORDER BY dependency_level DESC, object_name;
" -c imcust
```

**Expected: 5 views including the 4 intermediate ones**

### Check All Views Have DEV_DB References

```bash
snow sql -q "
SELECT
    object_name,
    CASE
        WHEN target_ddl LIKE '%DEV_DB%' AND target_ddl NOT LIKE '%PROD_DB%'
        THEN '✅ CORRECT'
        ELSE '❌ WRONG'
    END as validation
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = X
  AND object_type = 'VIEW'
ORDER BY object_name;
" -c imcust
```

**Expected: All 5 views show ✅ CORRECT**

### Check Only View DDLs Generated (No Tables)

```bash
snow sql -q "
SELECT object_type, COUNT(*) as count
FROM PROD_DB.ADMIN_SCHEMA.migration_ddl_scripts
WHERE migration_id = X
GROUP BY object_type;
" -c imcust
```

**Expected:**
```
OBJECT_TYPE | COUNT
VIEW        | 5
(NO TABLE entries)
```

---

## 🎯 What This Tests

| Scenario | Coverage |
|----------|----------|
| **View depends on views** | ✅ vw_transaction_analysis → 4 intermediate views |
| **Cross-schema references** | ✅ MART ↔ SRC tables |
| **Multi-level dependencies** | ✅ 3 dependency levels (0, 1, 2) |
| **PROD_DB → DEV_DB replacement** | ✅ All 5 views at all levels |
| **No table DDLs** | ✅ 0 table DDLs, 5 view DDLs |
| **CTAS before DDL** | ✅ Tables first, then views |

---

## 🚀 Quick Summary

**Just run these 2 commands:**

```bash
# 1. Recreate test data with new view structure
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)
snow sql -f IMCUST/00_create_test_dataset.sql -c imcust

# 2. Run your original migration command (no changes!)
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

**You should see:**
- ✅ 11 total objects discovered
- ✅ 5 view DDLs (including 4 intermediate views)
- ✅ 6 CTAS scripts (no table DDLs)
- ✅ All views with DEV_DB references

---

**Version:** 2.1 - View-on-View Dependencies
**Last Updated:** 2025-11-17
