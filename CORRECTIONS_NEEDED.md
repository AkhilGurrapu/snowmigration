# SQL Scripts Corrections Summary

## ✅ ALL CORRECTIONS COMPLETED (Updated 2025-11-10)

### SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
**Official Column Names (from Snowflake Documentation):**
- REFERENCED_DATABASE (not referenced_database_name)
- REFERENCED_SCHEMA (not referenced_schema_name)
- REFERENCED_OBJECT_NAME
- REFERENCED_OBJECT_ID
- REFERENCED_OBJECT_DOMAIN
- REFERENCING_DATABASE (not referencing_database_name)
- REFERENCING_SCHEMA (not referencing_schema_name)
- REFERENCING_OBJECT_NAME
- REFERENCING_OBJECT_ID
- REFERENCING_OBJECT_DOMAIN
- DEPENDENCY_TYPE

### Files Corrected:
1. ✅ IMCUST/MANUAL_01_discovery.sql - All OBJECT_DEPENDENCIES queries updated
2. ✅ IMCUST/AUTOMATED_migration_procedure.sql - All 3 procedures corrected
3. ✅ IMSDLC/MANUAL_04_validate.sql - All OBJECT_DEPENDENCIES queries updated
4. ✅ IMCUST/01_discovery_complete.sql - Obsolete file deleted
5. ✅ All procedures using SPLIT_TO_TABLE - Already using VALUE column correctly

### SPLIT_TO_TABLE Return Columns:
- SEQ - Sequence number
- INDEX - Element index (1-based)
- VALUE - The actual value

### Correct Usage (Verified in all scripts):
```sql
SELECT TRIM(VALUE) AS schema_name
FROM TABLE(SPLIT_TO_TABLE('schema1,schema2', ','))
```

## Validation Complete:
✅ All OBJECT_DEPENDENCIES column names corrected
✅ All SPLIT_TO_TABLE usages verified
✅ All IDENTIFIER() usages verified
✅ All GET_DDL() usages verified
✅ All INFORMATION_SCHEMA column names verified
✅ All recursive CTEs follow Snowflake syntax
✅ All stored procedures use correct syntax

## Latest Corrections (2025-11-10)

### ✅ DISTINCT in Recursive CTE Terms - FIXED

**Error:** `SQL compilation error: DISTINCT is not allowed in a CTEs recursive term`

**Root Cause:**
Snowflake does not allow DISTINCT in the recursive term (the SELECT after UNION ALL) of a recursive CTE.

**Files Corrected:**
1. ✅ `IMCUST/MANUAL_01_discovery.sql` - Removed DISTINCT from recursive terms (lines 91, 152)
2. ✅ `IMCUST/AUTOMATED_migration_procedure.sql` - Removed DISTINCT from recursive terms (lines 275, 305)

**What Changed:**
```sql
-- ❌ BEFORE (ERROR)
WITH RECURSIVE upstream_deps AS (
    SELECT DISTINCT ... -- OK in anchor
    UNION ALL
    SELECT DISTINCT ... -- ERROR: Not allowed in recursive term
)

-- ✅ AFTER (CORRECT)
WITH RECURSIVE upstream_deps AS (
    SELECT DISTINCT ... -- OK in anchor
    UNION ALL
    SELECT ... -- DISTINCT removed from recursive term
)
SELECT DISTINCT ... FROM upstream_deps; -- OK in final SELECT
```

---

## Validation Status

### Static Validation: ✅ PASSED
**Date:** 2025-11-10
**Tool:** `validate_sql_syntax.py`
**Result:** All 11 SQL files passed

### Connection Testing: ⏳ READY (Pending PAT)
**Tool:** `test_sql_scripts.py`
**Status:** Ready to run with PAT credentials

---

## Status: ✅ READY FOR CONNECTION TESTING

**Next Steps:**
1. Set PAT credentials (IMCUST_PAT, IMSDLC_PAT)
2. Run `python3 test_sql_scripts.py` for connection validation
3. Execute scripts in Snowflake UI after connection test passes

See `TESTING_GUIDE.md` for complete testing procedures.
See `SQL_SYNTAX_VALIDATION.md` for complete validation report.
