# Lineage-Preserving Cross-Account Migration Solution

## Executive Summary

This enhanced migration framework solves the **critical problem of missing native Snowflake lineage** after cross-account migrations by capturing and replicating the original transformation SQL used to populate derived tables.

### The Problem (Before)
- ❌ After migration, `GET_LINEAGE()` returns "No data" on target side
- ❌ Simple CTAS from shared database doesn't establish lineage relationships
- ❌ Transformation logic (JOINs, calculations) is lost
- ❌ Data governance and impact analysis impossible

### The Solution (After)
- ✅ Native Snowflake lineage preserved using INSERT...SELECT with original transformation SQL
- ✅ Captures transformation logic from ACCOUNT_USAGE.QUERY_HISTORY (365-day retention)
- ✅ Fallback strategies using ACCESS_HISTORY, metadata, and dependencies
- ✅ GET_LINEAGE() works correctly on target side
- ✅ Complete data governance and impact analysis maintained

---

## How It Works

### 4-Tier Hybrid Migration Strategy

#### **Tier 1: Object Classification**
Every object is classified to determine the appropriate migration strategy:

| Classification | Description | Migration Strategy |
|---------------|-------------|-------------------|
| **VIEW** | No data storage, only query logic | DDL only (no data migration) |
| **BASE_TABLE** | Tables with no upstream dependencies | CTAS from shared DB (lineage not needed) |
| **DERIVED_TABLE** | Tables with transformations/dependencies | INSERT with transformation SQL (PRESERVES LINEAGE!) |

#### **Tier 2: Transformation SQL Capture** (Primary)
Captures the original transformation SQL from `ACCOUNT_USAGE.QUERY_HISTORY` (365-day retention):

```sql
-- Example: Finding the original INSERT statement for FACT_TRANSACTIONS
SELECT query_id, query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%INSERT%INTO%fact_transactions%'
  AND query_text ILIKE '%SELECT%'
  AND execution_status = 'SUCCESS'
ORDER BY start_time DESC
LIMIT 1;
```

**Confidence Score**: 1.0 (exact match)

#### **Tier 3: Column Lineage Reconstruction** (Fallback)
Uses `ACCESS_HISTORY` view to reconstruct SQL from column lineage when query history is unavailable:

```sql
SELECT
    base_objects_accessed,  -- Source tables
    objects_modified         -- Target table with column mappings
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE objects_modified[0]:objectName::STRING ILIKE '%table_name%';
```

**Confidence Score**: 0.5 (reconstructed approximation)

#### **Tier 4: Metadata Extraction** (Last Resort)
Extracts transformation SQL from:
- Custom TAG: `TRANSFORMATION_SQL` (confidence: 0.8)
- Table COMMENT: If SQL is documented (confidence: 0.6)
- OBJECT_DEPENDENCIES: Build simple template (confidence: 0.3)

---

## Architecture Overview

### Source Account (IMCUST) - Enhanced Components

```
1. sp_get_upstream_dependencies
   └─> Discovers all dependencies using GET_LINEAGE

2. sp_classify_migration_objects (NEW!)
   └─> Classifies objects: BASE_TABLE, DERIVED_TABLE, VIEW

3. sp_capture_transformation_sql_enhanced (NEW!)
   └─> Captures SQL from QUERY_HISTORY (365 days)

4. sp_extract_lineage_from_metadata (NEW!)
   └─> Fallback: Extract from ACCESS_HISTORY, tags, comments

5. sp_generate_hybrid_migration_scripts (NEW!)
   └─> Generates INSERT...SELECT scripts with transformation logic

6. sp_generate_migration_scripts
   └─> Legacy DDL + CTAS (backward compatibility)

7. sp_setup_data_share
   └─> Creates share with database role

8. sp_orchestrate_migration
   └─> Main orchestrator (calls all procedures)
```

### Target Account (IMSDLC) - Enhanced Components

```
1. sp_execute_target_ddl
   └─> Creates all object structures

2. sp_execute_hybrid_migration (NEW!)
   └─> Executes INSERT...SELECT with transformation SQL
   └─> PRESERVES NATIVE LINEAGE!

3. sp_execute_target_ctas
   └─> Legacy CTAS fallback

4. sp_execute_full_migration
   └─> Main orchestrator (auto-detects hybrid scripts)
```

---

## New Metadata Tables

### `migration_share_objects` (Enhanced)
```sql
...existing columns...
object_classification VARCHAR  -- NEW: BASE_TABLE, DERIVED_TABLE, VIEW
```

### `migration_transformation_sql` (NEW!)
```sql
migration_id NUMBER
source_database VARCHAR
source_schema VARCHAR
object_name VARCHAR
object_type VARCHAR
transformation_sql VARCHAR       -- Original INSERT/MERGE/CTAS SQL
capture_method VARCHAR           -- QUERY_HISTORY, ACCESS_HISTORY, COMMENT, TAG, NONE
query_id VARCHAR                 -- Reference to source query
confidence_score NUMBER          -- 1.0=exact, 0.5=reconstructed, 0.0=fallback
created_ts TIMESTAMP_LTZ
```

### `migration_hybrid_scripts` (NEW!)
```sql
migration_id NUMBER
source_database VARCHAR
source_schema VARCHAR
object_name VARCHAR
object_type VARCHAR
object_classification VARCHAR
migration_strategy VARCHAR       -- CTAS_FROM_SHARED, INSERT_WITH_TRANSFORMATION, VIEW_ONLY
migration_script VARCHAR         -- The actual SQL to execute
execution_order NUMBER
created_ts TIMESTAMP_LTZ
```

---

## Complete Migration Workflow

### **Step 1: One-Time Setup (Source Account)**

```bash
# Set environment
export SNOWFLAKE_PASSWORD=$(cat .env.imcust_pat)

# Deploy enhanced framework
snow sql -f IMCUST/01_setup_config_tables.sql -c imcust  # Enhanced with new tables
snow sql -f IMCUST/02_sp_get_upstream_dependencies.sql -c imcust
snow sql -f IMCUST/03_sp_generate_migration_scripts.sql -c imcust
snow sql -f IMCUST/04_sp_setup_data_share.sql -c imcust
snow sql -f IMCUST/05_sp_orchestrate_migration.sql -c imcust  # Enhanced orchestrator

# NEW: Deploy lineage preservation procedures
snow sql -f IMCUST/06_sp_classify_migration_objects.sql -c imcust
snow sql -f IMCUST/07_sp_capture_transformation_sql_enhanced.sql -c imcust
snow sql -f IMCUST/08_sp_extract_lineage_from_metadata.sql -c imcust
snow sql -f IMCUST/09_sp_generate_hybrid_migration_scripts.sql -c imcust
```

### **Step 2: One-Time Setup (Target Account)**

```bash
# Set environment
export SNOWFLAKE_PASSWORD=$(cat .env.imsdlc_pat)

# Deploy enhanced framework
snow sql -f IMSDLC/01_setup_execution_log.sql -c imsdlc
snow sql -f IMSDLC/02_sp_execute_target_ddl_v2.sql -c imsdlc
snow sql -f IMSDLC/03_sp_execute_target_ctas_v2.sql -c imsdlc
snow sql -f IMSDLC/04_sp_execute_full_migration.sql -c imsdlc  # Enhanced orchestrator

# NEW: Deploy hybrid migration procedure
snow sql -f IMSDLC/05_sp_execute_hybrid_migration.sql -c imsdlc
```

### **Step 3: Execute Migration (Source Side)**

```sql
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
    'IMSDLC'                            -- Target account
);
```

**Enhanced Output:**
```
Migration ID: 2

Dependency Discovery: Found 14 objects across 2 dependency levels

Object Classification Summary:
  BASE_TABLE: 5 objects
  DERIVED_TABLE: 4 objects
  VIEW: 5 objects

Transformation SQL Capture Summary:
  Successfully captured: 4 objects
  No SQL found: 0 objects

Metadata Extraction Summary (Fallback):
  Extracted from metadata: 0 objects
  No metadata found: 0 objects

Hybrid Migration Script Generation Summary:
  VIEW_ONLY: 5 objects
  CTAS_FROM_SHARED: 5 objects
  INSERT_WITH_TRANSFORMATION: 4 objects (LINEAGE PRESERVED!)

✓ SUCCESS: 4 objects will have native Snowflake lineage preserved!
```

### **Step 4: Create Shared Database (Target Side)**

```sql
CREATE DATABASE IF NOT EXISTS IMCUST_SHARED_DB
FROM SHARE NFMYIZV.IMCUST.IMCUST_TO_IMSDLC_SHARE;

GRANT IMPORTED PRIVILEGES ON DATABASE IMCUST_SHARED_DB TO ROLE ACCOUNTADMIN;
```

### **Step 5: Execute Migration (Target Side)**

```sql
CALL DEV_DB.ADMIN_SCHEMA.sp_execute_full_migration(
    2,                      -- migration_id
    'IMCUST_SHARED_DB',     -- Shared database
    'ADMIN_SCHEMA',         -- Admin schema
    'DEV_DB',              -- Target database
    'ADMIN_SCHEMA',        -- Target admin schema
    TRUE                    -- Validate before execution
);
```

**Enhanced Output:**
```
Starting migration 2 from shared database IMCUST_SHARED_DB
✓ Using HYBRID migration (preserves native lineage)

DDL Execution Complete: 14 succeeded, 0 failed

Hybrid Migration Execution Complete:
  4 succeeded
  0 failed
  5 skipped (views)

✓ SUCCESS: Native Snowflake lineage should now be preserved!
  Verify with: SELECT * FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(...))
```

### **Step 6: Validate Lineage Preservation**

```sql
-- Test lineage on a derived table
SELECT
    SOURCE_OBJECT_NAME,
    SOURCE_OBJECT_DOMAIN,
    DISTANCE
FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(
    'DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS',
    'TABLE',
    'UPSTREAM'
));
```

**Expected Result:**
```
SOURCE_OBJECT_NAME                                    SOURCE_OBJECT_DOMAIN    DISTANCE
-----------------------------------------------------  ----------------------  --------
DEV_DB.SRC_INVESTMENTS_BOLT.TRANSACTIONS_RAW          TABLE                   1
DEV_DB.SRC_INVESTMENTS_BOLT.BROKER_MASTER             TABLE                   1
```

**Before (Broken):**
```
No data returned
```

---

## Key Benefits

### ✅ **Native Lineage Preserved**
- GET_LINEAGE() works correctly on target side
- Upstream dependencies correctly tracked
- Column-level lineage maintained

### ✅ **Data Governance Maintained**
- Impact analysis possible
- Compliance reporting intact
- Sensitive data tracking preserved

### ✅ **Organic Transformation Logic**
- Uses actual business logic from source
- Preserves JOINs, calculations, CASE statements
- Not just copying data - replicating data flow

### ✅ **Multiple Fallback Strategies**
- Query history (365 days)
- Access history
- Metadata (tags/comments)
- Object dependencies
- Graceful degradation to CTAS if needed

### ✅ **Backward Compatible**
- Legacy CTAS still available
- Auto-detects hybrid scripts
- No breaking changes to existing migrations

---

## Technical Deep Dive

### Why CTAS from Shared DB Breaks Lineage

From Snowflake documentation:
> "If a data sharing consumer moves data from the shared view to a table, Snowflake does not record the view columns as baseSources for the newly created table."

**Current (Broken) Approach:**
```sql
CREATE TABLE target_table AS
SELECT * FROM <SHARED_DB>.source_table;
```
Result: ❌ No lineage established (source is shared DB)

**Enhanced (Working) Approach:**
```sql
-- Step 1: Create empty table structure
CREATE TABLE target_table (...);

-- Step 2: Populate with transformation SQL (from captured query history)
INSERT INTO target_table
SELECT
    t.col1,
    t.col2,
    b.col3,
    ROUND(t.amount * b.rate, 2) as calculated_col
FROM target_db.schema.upstream_table1 t
JOIN target_db.schema.upstream_table2 b ON t.id = b.id;
```
Result: ✅ Lineage established (source is regular tables in target account)

### Confidence Scoring

| Capture Method | Score | Description |
|---------------|-------|-------------|
| QUERY_HISTORY_INSERT | 1.0 | Exact INSERT statement found |
| QUERY_HISTORY_MERGE | 1.0 | Exact MERGE statement found |
| QUERY_HISTORY_CTAS | 0.9 | CTAS found (converted to INSERT) |
| TAG | 0.8 | SQL from TRANSFORMATION_SQL tag |
| COMMENT | 0.6 | SQL extracted from table comment |
| ACCESS_HISTORY | 0.5 | Reconstructed from column lineage |
| OBJECT_DEPENDENCIES | 0.3 | Generic template from dependencies |
| NONE | 0.0 | No transformation SQL found (fallback to CTAS) |

---

## Troubleshooting

### Issue: Transformation SQL Not Captured

**Symptoms:**
- Hybrid scripts show CTAS_FROM_SHARED for derived tables
- Confidence score is 0.0 or NULL

**Causes:**
1. Query history retention exceeded (>365 days)
2. Table was created via external tool (not via Snowflake SQL)
3. ACCOUNT_USAGE views not accessible

**Solutions:**
1. **Document transformation SQL in metadata:**
   ```sql
   -- Option 1: Use TAG
   CREATE TAG TRANSFORMATION_SQL;
   ALTER TABLE fact_transactions SET TAG TRANSFORMATION_SQL = 'INSERT INTO fact_transactions SELECT ...';

   -- Option 2: Use COMMENT
   COMMENT ON TABLE fact_transactions IS 'INSERT INTO fact_transactions SELECT ...';
   ```

2. **Re-run data load to capture in query history:**
   ```sql
   TRUNCATE TABLE fact_transactions;
   INSERT INTO fact_transactions SELECT ...; -- This will be captured
   ```

3. **Manual override in hybrid scripts table:**
   ```sql
   UPDATE migration_transformation_sql
   SET transformation_sql = 'INSERT INTO fact_transactions SELECT ...',
       capture_method = 'MANUAL',
       confidence_score = 1.0
   WHERE migration_id = 2 AND object_name = 'FACT_TRANSACTIONS';
   ```

### Issue: Lineage Still Missing After Migration

**Diagnostic Query:**
```sql
-- Check hybrid script strategy
SELECT object_name, migration_strategy, confidence_score
FROM IMCUST_SHARED_DB.ADMIN_SCHEMA.migration_transformation_sql
WHERE migration_id = 2;

-- Check execution log
SELECT object_name, script_type, status, error_message
FROM DEV_DB.ADMIN_SCHEMA.migration_execution_log
WHERE migration_id = 2 AND execution_phase = 'HYBRID_MIGRATION';
```

---

## Production Recommendations

### Before Migration

1. ✅ **Test lineage on source:**
   ```sql
   SELECT * FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('PROD_DB.SCHEMA.TABLE', 'TABLE', 'UPSTREAM'));
   ```

2. ✅ **Verify query history accessibility:**
   ```sql
   SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE start_time >= DATEADD(day, -365, CURRENT_TIMESTAMP());
   ```

3. ✅ **Document critical transformations in metadata** (tags/comments)

4. ✅ **Review confidence scores** after script generation

### After Migration

1. ✅ **Validate lineage for all derived tables:**
   ```sql
   SELECT object_name, migration_strategy
   FROM migration_hybrid_scripts
   WHERE migration_id = ? AND object_classification = 'DERIVED_TABLE';
   ```

2. ✅ **Compare row counts:**
   ```sql
   SELECT
       'table_name' as table_name,
       (SELECT COUNT(*) FROM shared_db.schema.table_name) as source_count,
       (SELECT COUNT(*) FROM target_db.schema.table_name) as target_count;
   ```

3. ✅ **Test GET_LINEAGE() on critical objects**

4. ✅ **Review execution log for failures**

---

## Summary

This enhanced migration framework **solves the critical lineage tracking problem** by:

1. **Capturing** the original transformation SQL from query history (365 days)
2. **Classifying** objects to determine appropriate migration strategy
3. **Replicating** organic data flow using INSERT...SELECT with captured SQL
4. **Establishing** native Snowflake lineage relationships on target side
5. **Maintaining** data governance and impact analysis capabilities

**Result:** GET_LINEAGE() works correctly on target side, preserving complete data lineage for compliance, governance, and impact analysis.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024-11 | Initial framework with CTAS from shared DB |
| 2.0 | 2024-11 | Cross-schema dependency fix |
| **3.0** | **2024-11** | **Lineage preservation with hybrid migration** |

**Author:** Enhanced Migration Framework v3.0
**License:** Internal Use Only
