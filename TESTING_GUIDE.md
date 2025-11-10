# SQL Scripts Testing Guide

## ✅ Current Validation Status

**Date:** 2025-11-10
**Status:** ALL SCRIPTS VALIDATED AND READY FOR TESTING

---

## Recent Fixes Applied

### 1. ✅ FIXED: DISTINCT in Recursive CTE Terms
**Error:** `SQL compilation error: DISTINCT is not allowed in a CTEs recursive term`

**Files Fixed:**
- `IMCUST/MANUAL_01_discovery.sql` (lines 91, 152)
- `IMCUST/AUTOMATED_migration_procedure.sql` (lines 275, 305)

**Fix Applied:**
```sql
-- ❌ WRONG (DISTINCT in recursive term)
UNION ALL
SELECT DISTINCT
    od.REFERENCING_DATABASE,
    ...
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
INNER JOIN upstream_deps ud

-- ✅ CORRECT (DISTINCT removed from recursive term)
UNION ALL
SELECT
    od.REFERENCING_DATABASE,
    ...
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES od
INNER JOIN upstream_deps ud
```

**Note:** DISTINCT is still correctly used in:
- Anchor clauses (first SELECT in recursive CTE) ✅
- Final SELECT from CTE ✅

### 2. ✅ PREVIOUSLY FIXED: OBJECT_DEPENDENCIES Column Names
**Files Fixed:**
- `IMCUST/MANUAL_01_discovery.sql`
- `IMCUST/AUTOMATED_migration_procedure.sql`
- `IMSDLC/MANUAL_04_validate.sql`

**Correct Column Names:**
- `REFERENCED_DATABASE` (not `referenced_database_name`)
- `REFERENCED_SCHEMA` (not `referenced_schema_name`)
- `REFERENCING_DATABASE` (not `referencing_database_name`)
- `REFERENCING_SCHEMA` (not `referencing_schema_name`)

---

## Static Validation Results

**Validation Tool:** `validate_sql_syntax.py`
**Run Date:** 2025-11-10
**Result:** ✅ PASSED (warnings are false positives)

### Files Validated:

#### IMCUST (Source Account)
| File | Status | Notes |
|------|--------|-------|
| `MANUAL_01_discovery.sql` | ✅ PASS | No errors or warnings |
| `MANUAL_02_extract_ddl.sql` | ✅ PASS | Warning is false positive |
| `MANUAL_03_create_share.sql` | ✅ PASS | No errors or warnings |
| `MANUAL_04_cleanup.sql` | ✅ PASS | No errors or warnings |
| `AUTOMATED_migration_procedure.sql` | ✅ PASS | Warnings are false positives |

#### IMSDLC (Target Account)
| File | Status | Notes |
|------|--------|-------|
| `MANUAL_01_consume_share.sql` | ✅ PASS | No errors or warnings |
| `MANUAL_02_create_objects.sql` | ✅ PASS | No errors or warnings |
| `MANUAL_03_populate_data.sql` | ✅ PASS | No errors or warnings |
| `MANUAL_04_validate.sql` | ✅ PASS | No errors or warnings |
| `MANUAL_05_cleanup.sql` | ✅ PASS | No errors or warnings |
| `AUTOMATED_migration_procedure.sql` | ✅ PASS | Warning is false positive |

### Validation Checks Performed:
- ✅ DISTINCT in recursive CTE terms
- ✅ OBJECT_DEPENDENCIES column names
- ✅ SPLIT_TO_TABLE usage (VALUE column)
- ✅ IDENTIFIER() syntax
- ✅ GET_DDL() syntax
- ✅ Basic SQL syntax

---

## Connection Testing (Requires PAT Credentials)

### Prerequisites

1. **Install Snowflake Python Connector:**
   ```bash
   pip3 install snowflake-connector-python --user
   ```

2. **Set PAT Credentials:**
   ```bash
   # Option 1: Environment variables
   export IMCUST_PAT='your_imcust_pat_token_here'
   export IMSDLC_PAT='your_imsdlc_pat_token_here'

   # Option 2: Create .env files
   echo 'IMCUST_PAT=your_token_here' > .env.imcust_pat
   echo 'IMSDLC_PAT=your_token_here' > .env.imsdlc_pat
   source .env.imcust_pat
   source .env.imsdlc_pat
   ```

### Running Connection Tests

#### Static Validation (No credentials needed):
```bash
python3 validate_sql_syntax.py
```

#### Full Connection Test (Requires PAT):
```bash
python3 test_sql_scripts.py
```

### What the Connection Test Does:

1. **Connects to both Snowflake accounts** using PAT authentication
2. **Parses each SQL file** into individual statements
3. **Validates syntax** using Snowflake's EXPLAIN command
4. **Reports results** for each statement in each file
5. **Provides summary** of all validation results

### Expected Output:

```
================================================================================
Snowflake SQL Scripts Validation
================================================================================

================================================================================
TESTING IMCUST (SOURCE) SCRIPTS
================================================================================
✓ Connected to Snowflake account: nfmyizv-imcust

================================================================================
Testing: MANUAL_01_discovery.sql
================================================================================
Found 8 statements to validate

  Statement 1: USE ROLE ACCOUNTADMIN;
  ◉ Statement 1: SKIPPED (not validatable)

  Statement 2: USE WAREHOUSE ADMIN_WH;
  ◉ Statement 2: SKIPPED (not validatable)

  Statement 3: USE DATABASE PROD_DB;
  ◉ Statement 3: SKIPPED (not validatable)

  Statement 4: SELECT table_catalog AS database_name, table_schema...
  ✓ Statement 4: VALID

[... continues for all statements and files ...]

================================================================================
FINAL SUMMARY
================================================================================

IMCUST Scripts:
  ✓ PASS MANUAL_01_discovery.sql
  ✓ PASS MANUAL_02_extract_ddl.sql
  ✓ PASS MANUAL_03_create_share.sql
  ✓ PASS MANUAL_04_cleanup.sql
  ✓ PASS AUTOMATED_migration_procedure.sql

IMSDLC Scripts:
  ✓ PASS MANUAL_01_consume_share.sql
  ✓ PASS MANUAL_02_create_objects.sql
  ✓ PASS MANUAL_03_populate_data.sql
  ✓ PASS MANUAL_04_validate.sql
  ✓ PASS MANUAL_05_cleanup.sql
  ✓ PASS AUTOMATED_migration_procedure.sql

================================================================================
✓✓✓ ALL SCRIPTS VALIDATED SUCCESSFULLY ✓✓✓
================================================================================
```

---

## Manual Testing in Snowflake UI

If you prefer to test manually in SnowSight:

### IMCUST Account Testing:

1. **Login to SnowSight:**
   - Account: `nfmyizv-imcust`
   - User: `svc4snowflakedeploy`
   - Role: `ACCOUNTADMIN`

2. **Test Discovery Script:**
   ```sql
   -- Copy/paste from IMCUST/MANUAL_01_discovery.sql
   -- Run each section separately
   ```

3. **Test DDL Extraction:**
   ```sql
   -- Copy/paste from IMCUST/MANUAL_02_extract_ddl.sql
   ```

4. **Test Share Creation:**
   ```sql
   -- Copy/paste from IMCUST/MANUAL_03_create_share.sql
   ```

### IMSDLC Account Testing:

1. **Login to SnowSight:**
   - Account: `nfmyizv-imsdlc`
   - User: `svc4snowflakedeploy`
   - Role: `ACCOUNTADMIN`

2. **Test Share Consumption:**
   ```sql
   -- Copy/paste from IMSDLC/MANUAL_01_consume_share.sql
   ```

3. **Test Object Creation:**
   ```sql
   -- Copy/paste from IMSDLC/MANUAL_02_create_objects.sql
   ```

4. **Test Data Population:**
   ```sql
   -- Copy/paste from IMSDLC/MANUAL_03_populate_data.sql
   ```

5. **Test Validation:**
   ```sql
   -- Copy/paste from IMSDLC/MANUAL_04_validate.sql
   ```

---

## Troubleshooting

### Common Issues:

#### 1. "Network is unreachable" or connection timeout
**Solution:** Check Snowflake account URL and network connectivity

#### 2. "Authentication failed"
**Solution:** Verify PAT token is correct and not expired

#### 3. "Object does not exist" errors
**Solution:** Ensure you're running scripts in the correct order:
- IMCUST scripts 1-3 must complete before IMSDLC scripts
- Share must exist in IMCUST before consuming in IMSDLC
- Shared database must exist before creating objects

#### 4. "Insufficient privileges"
**Solution:** Ensure you're using ACCOUNTADMIN role

---

## Next Steps

### After Validation Passes:

1. **Review Execution Plan:**
   - See `EXECUTION_GUIDE.sql` for step-by-step migration plan

2. **Run Migration:**
   - Execute IMCUST scripts in order (1-4)
   - Execute IMSDLC scripts in order (1-5)
   - Run validation checks after each step

3. **Monitor Progress:**
   - Check row counts
   - Verify object dependencies
   - Validate data integrity

4. **Document Results:**
   - Record migration metrics
   - Note any issues encountered
   - Update validation documentation

---

## Files Reference

### Testing Tools:
- `validate_sql_syntax.py` - Static syntax validation (no credentials)
- `test_sql_scripts.py` - Full connection testing (requires PAT)

### SQL Scripts:
- `IMCUST/MANUAL_*.sql` - Manual migration scripts (source)
- `IMCUST/AUTOMATED_migration_procedure.sql` - Automated procedures (source)
- `IMSDLC/MANUAL_*.sql` - Manual migration scripts (target)
- `IMSDLC/AUTOMATED_migration_procedure.sql` - Automated procedures (target)

### Documentation:
- `TESTING_GUIDE.md` (this file) - Testing procedures
- `SQL_SYNTAX_VALIDATION.md` - Syntax validation report
- `CORRECTIONS_NEEDED.md` - Fixes applied
- `EXECUTION_GUIDE.sql` - Step-by-step execution plan
- `FINAL_STATUS.md` - Quick reference status

---

## Validation History

| Date | Issue | Status | Files Affected |
|------|-------|--------|----------------|
| 2025-11-10 | DISTINCT in recursive CTE | ✅ FIXED | MANUAL_01_discovery.sql, AUTOMATED_migration_procedure.sql |
| 2025-11-10 | OBJECT_DEPENDENCIES columns | ✅ FIXED | MANUAL_01_discovery.sql, AUTOMATED_migration_procedure.sql, MANUAL_04_validate.sql |
| 2025-11-10 | Static validation | ✅ PASSED | All 11 SQL files |

---

**Status:** ✅ ALL SCRIPTS READY FOR PRODUCTION TESTING
**Last Updated:** 2025-11-10
**Next Action:** Set PAT credentials and run `python3 test_sql_scripts.py`
