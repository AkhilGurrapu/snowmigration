# ⚠️ EXECUTION ORDER GUIDE - CRITICAL ⚠️

## Root Cause of Your Errors

**Error:** "Database 'MIGRATION_SHARED_DB' does not exist or not authorized"

**Reason:** You're trying to run procedures that access `MIGRATION_SHARED_DB` BEFORE creating it!

---

## ✅ CORRECT EXECUTION ORDER

### PHASE 1: CREATE PROCEDURES (One-time setup)

```sql
-- Run these files ONCE to create the stored procedures

-- 1. Create IMCUST procedures
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE PROD_DB;
@IMCUST/AUTOMATED_migration_procedure.sql

-- 2. Create IMSDLC procedures
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE DEV_DB;
@IMSDLC/AUTOMATED_migration_procedure.sql
```

### PHASE 2: IMCUST - Prepare Share

```sql
-- 3. Prepare migration share in IMCUST
CALL PROD_DB.PUBLIC.SP_PREPARE_MIGRATION_SHARE(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS',
    'MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'nfmyizv.imsdlc'
);
```

### PHASE 3: IMSDLC - Consume Share (MUST RUN FIRST!)

```sql
-- 4. Consume the share to CREATE MIGRATION_SHARED_DB
-- ⚠️ THIS STEP IS REQUIRED BEFORE ANY OTHER IMSDLC PROCEDURES!
CALL DEV_DB.PUBLIC.SP_CONSUME_SHARE(
    'nfmyizv.imcust.MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'MIGRATION_SHARED_DB'
);

-- ✅ After this, MIGRATION_SHARED_DB will exist!
-- ✅ Now you can run the other procedures
```

### PHASE 4: IMSDLC - Create Tables

```sql
-- 5. Create table structures from share
CALL DEV_DB.PUBLIC.SP_CREATE_TABLES_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE  -- FALSE = structure only, TRUE = structure + data
);
```

### PHASE 5: IMSDLC - Populate Data

```sql
-- 6. Populate data from share
CALL DEV_DB.PUBLIC.SP_POPULATE_DATA_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE  -- FALSE = don't truncate, TRUE = truncate before load
);
```

### PHASE 6: IMSDLC - Validate

```sql
-- 7. Validate migration
CALL DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);
```

### PHASE 7: IMSDLC - Complete Workflow (All-in-One)

```sql
-- 8. OR run the complete workflow (does steps 4-7 automatically)
CALL DEV_DB.PUBLIC.SP_COMPLETE_MIGRATION_WORKFLOW(
    'nfmyizv.imcust.MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE,  -- CREATE_DATA (FALSE = structure only)
    FALSE   -- TRUNCATE_BEFORE_LOAD
);
```

---

## ⚠️ YOUR SPECIFIC ERROR

You ran procedures in PHASE 5 and 6 **WITHOUT** running PHASE 3 first!

```
❌ You tried: SP_CREATE_TABLES_FROM_SHARE
❌ You tried: SP_POPULATE_DATA_FROM_SHARE
❌ Error: Database 'MIGRATION_SHARED_DB' does not exist

✅ You MUST run: SP_CONSUME_SHARE FIRST!
```

---

## Quick Fix - Run This NOW:

```sql
-- Connect to IMSDLC account
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE ADMIN_WH;
USE DATABASE DEV_DB;

-- 1. First, create MIGRATION_SHARED_DB by consuming the share
CALL DEV_DB.PUBLIC.SP_CONSUME_SHARE(
    'nfmyizv.imcust.MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'MIGRATION_SHARED_DB'
);

-- 2. Verify the database exists
SHOW DATABASES LIKE 'MIGRATION_SHARED_DB';

-- 3. Now run your other procedures
CALL DEV_DB.PUBLIC.SP_CREATE_TABLES_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE
);

CALL DEV_DB.PUBLIC.SP_POPULATE_DATA_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE
);

CALL DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);
```

---

## Verification Commands

```sql
-- After SP_CONSUME_SHARE, verify shared database exists:
SHOW DATABASES LIKE 'MIGRATION_SHARED_DB';

-- List schemas in shared database:
SHOW SCHEMAS IN DATABASE MIGRATION_SHARED_DB;

-- List tables in shared database:
SHOW TABLES IN DATABASE MIGRATION_SHARED_DB;

-- Verify you can query shared tables:
SELECT COUNT(*) FROM MIGRATION_SHARED_DB.SRC_INVESTMENTS_BOLT.STOCK_METADATA_RAW;
```

---

## Summary

**Your scripts have NO SYNTAX ERRORS! ✅**

You just need to follow the execution order:

1. ✅ Create procedures (run SQL files)
2. ✅ Prepare share in IMCUST (SP_PREPARE_MIGRATION_SHARE)
3. ⚠️ **CONSUME SHARE in IMSDLC (SP_CONSUME_SHARE) ← YOU SKIPPED THIS!**
4. ✅ Create tables (SP_CREATE_TABLES_FROM_SHARE)
5. ✅ Populate data (SP_POPULATE_DATA_FROM_SHARE)
6. ✅ Validate (SP_VALIDATE_MIGRATION)

**Run SP_CONSUME_SHARE first, and all other procedures will work!**
