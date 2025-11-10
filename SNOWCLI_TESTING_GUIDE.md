# SnowCLI Testing Guide for SQL Migration Scripts

## ✅ FIXES APPLIED (2025-11-10)

### Critical Errors Fixed:
1. **RECORD Type Error** - FIXED in 3 locations:
   - `IMCUST/AUTOMATED_migration_procedure.sql` (line 77) ✅
   - `IMSDLC/AUTOMATED_migration_procedure.sql` (line 69, 139) ✅

2. **DISTINCT in Recursive CTE** - FIXED in 4 locations:
   - `IMCUST/MANUAL_01_discovery.sql` (lines 91, 152) ✅
   - `IMCUST/AUTOMATED_migration_procedure.sql` (lines 275, 305) ✅

**All SQL files are now ready for testing!**

---

## Prerequisites

### 1. Install SnowCLI
```bash
pip3 install --user snowflake-cli-labs
snow --version  # Should show: Snowflake CLI version: 3.13.0+
```

### 2. Configure Connections

Create Snowflake connection configuration:

```bash
# Configure IMCUST connection
snow connection add \
  --connection-name imcust \
  --account nfmyizv-imcust \
  --user svc4snowflakedeploy \
  --authenticator snowflake_jwt \
  --private-key-path ~/.snowflake/imcust_key.pem \
  --role ACCOUNTADMIN \
  --warehouse admin_wh \
  --database prod_db

# OR using token authentication
snow connection add \
  --connection-name imcust \
  --account nfmyizv-imcust \
  --user svc4snowflakedeploy \
  --token $IMCUST_PAT \
  --role ACCOUNTADMIN \
  --warehouse admin_wh \
  --database prod_db

# Configure IMSDLC connection
snow connection add \
  --connection-name imsdlc \
  --account nfmyizv-imsdlc \
  --user svc4snowflakedeploy \
  --token $IMSDLC_PAT \
  --role ACCOUNTADMIN \
  --warehouse admin_wh \
  --database dev_db
```

### 3. Verify Connections
```bash
snow connection test --connection imcust
snow connection test --connection imsdlc
```

---

## Testing IMCUST (Source) Scripts

### Test 1: Discovery Script
```bash
echo "Testing MANUAL_01_discovery.sql..."
snow sql \
  --connection imcust \
  --filename IMCUST/MANUAL_01_discovery.sql \
  --output json

# Expected: Should return list of tables and dependencies
```

### Test 2: Extract DDL Script
```bash
echo "Testing MANUAL_02_extract_ddl.sql..."
snow sql \
  --connection imcust \
  --filename IMCUST/MANUAL_02_extract_ddl.sql \
  --output json

# Expected: Should return CREATE TABLE/VIEW/PROCEDURE statements
```

### Test 3: Create Share Script
```bash
echo "Testing MANUAL_03_create_share.sql..."
snow sql \
  --connection imcust \
  --filename IMCUST/MANUAL_03_create_share.sql

# Expected: Share created successfully
```

### Test 4: Cleanup Script (DO NOT RUN YET)
```bash
# WARNING: This drops the share - only run after migration complete
# snow sql --connection imcust --filename IMCUST/MANUAL_04_cleanup.sql
```

### Test 5: Automated Migration Procedures
```bash
echo "Testing AUTOMATED_migration_procedure.sql..."

# Create the procedures
snow sql \
  --connection imcust \
  --filename IMCUST/AUTOMATED_migration_procedure.sql

# Test SP_PREPARE_MIGRATION_SHARE
snow sql \
  --connection imcust \
  --query "CALL PROD_DB.PUBLIC.SP_PREPARE_MIGRATION_SHARE(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS',
    'MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'nfmyizv.imsdlc'
);"

# Test SP_EXTRACT_ALL_DDL
snow sql \
  --connection imcust \
  --query "CALL PROD_DB.PUBLIC.SP_EXTRACT_ALL_DDL(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'TABLE,VIEW,PROCEDURE'
);"

# Test SP_DISCOVER_DEPENDENCIES
snow sql \
  --connection imcust \
  --query "CALL PROD_DB.PUBLIC.SP_DISCOVER_DEPENDENCIES(
    'PROD_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS,VW_CURRENT_HOLDINGS',
    10
);"
```

---

## Testing IMSDLC (Target) Scripts

### Test 1: Consume Share Script
```bash
echo "Testing MANUAL_01_consume_share.sql..."
snow sql \
  --connection imsdlc \
  --filename IMSDLC/MANUAL_01_consume_share.sql

# Expected: Shared database created
```

### Test 2: Create Objects Script
```bash
echo "Testing MANUAL_02_create_objects.sql..."

# First, extract DDL from IMCUST and save to file
snow sql \
  --connection imcust \
  --filename IMCUST/MANUAL_02_extract_ddl.sql \
  --output json > ddl_output.json

# Then manually edit MANUAL_02_create_objects.sql with the DDL
# and execute:
snow sql \
  --connection imsdlc \
  --filename IMSDLC/MANUAL_02_create_objects.sql
```

### Test 3: Populate Data Script
```bash
echo "Testing MANUAL_03_populate_data.sql..."
snow sql \
  --connection imsdlc \
  --filename IMSDLC/MANUAL_03_populate_data.sql

# Expected: Data copied from shared tables
```

### Test 4: Validation Script
```bash
echo "Testing MANUAL_04_validate.sql..."
snow sql \
  --connection imsdlc \
  --filename IMSDLC/MANUAL_04_validate.sql \
  --output json

# Expected: 10 validation checks pass
```

### Test 5: Cleanup Script (DO NOT RUN YET)
```bash
# WARNING: This drops the shared database - only run after validation complete
# snow sql --connection imsdlc --filename IMSDLC/MANUAL_05_cleanup.sql
```

### Test 6: Automated Migration Procedures
```bash
echo "Testing AUTOMATED_migration_procedure.sql..."

# Create the procedures
snow sql \
  --connection imsdlc \
  --filename IMSDLC/AUTOMATED_migration_procedure.sql

# Test SP_CONSUME_SHARE
snow sql \
  --connection imsdlc \
  --query "CALL DEV_DB.PUBLIC.SP_CONSUME_SHARE(
    'nfmyizv.imcust.MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'MIGRATION_SHARED_DB'
);"

# Test SP_CREATE_TABLES_FROM_SHARE
snow sql \
  --connection imsdlc \
  --query "CALL DEV_DB.PUBLIC.SP_CREATE_TABLES_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE
);"

# Test SP_POPULATE_DATA_FROM_SHARE
snow sql \
  --connection imsdlc \
  --query "CALL DEV_DB.PUBLIC.SP_POPULATE_DATA_FROM_SHARE(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    FALSE
);"

# Test SP_VALIDATE_MIGRATION
snow sql \
  --connection imsdlc \
  --query "CALL DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);"
```

---

## Quick Syntax Validation (No Execution)

To check SQL syntax without executing:

```bash
# Validate all IMCUST scripts
for file in IMCUST/*.sql; do
  echo "Validating $file..."
  snow sql --connection imcust --filename "$file" --dry-run 2>&1 | grep -i "error"
done

# Validate all IMSDLC scripts
for file in IMSDLC/*.sql; do
  echo "Validating $file..."
  snow sql --connection imsdlc --filename "$file" --dry-run 2>&1 | grep -i "error"
done
```

---

## Automated Testing Script

Create a test runner script:

```bash
#!/bin/bash
# test_all_scripts.sh

set -e

echo "=========================================="
echo "TESTING IMCUST (SOURCE) SCRIPTS"
echo "=========================================="

echo "Test 1: Discovery..."
snow sql --connection imcust --filename IMCUST/MANUAL_01_discovery.sql --output json > /dev/null && echo "✓ PASS" || echo "✗ FAIL"

echo "Test 2: Extract DDL..."
snow sql --connection imcust --filename IMCUST/MANUAL_02_extract_ddl.sql --output json > /dev/null && echo "✓ PASS" || echo "✗ FAIL"

echo "Test 3: Create Share..."
snow sql --connection imcust --filename IMCUST/MANUAL_03_create_share.sql > /dev/null && echo "✓ PASS" || echo "✗ FAIL"

echo "Test 4: Create Procedures..."
snow sql --connection imcust --filename IMCUST/AUTOMATED_migration_procedure.sql > /dev/null && echo "✓ PASS" || echo "✗ FAIL"

echo ""
echo "=========================================="
echo "TESTING IMSDLC (TARGET) SCRIPTS"
echo "=========================================="

echo "Test 1: Consume Share..."
snow sql --connection imsdlc --filename IMSDLC/MANUAL_01_consume_share.sql > /dev/null && echo "✓ PASS" || echo "✗ FAIL"

echo "Test 2: Create Procedures..."
snow sql --connection imsdlc --filename IMSDLC/AUTOMATED_migration_procedure.sql > /dev/null && echo "✓ PASS" || echo "✗ FAIL"

echo "Test 3: Validation Script..."
snow sql --connection imsdlc --filename IMSDLC/MANUAL_04_validate.sql --output json > /dev/null && echo "✓ PASS" || echo "✗ FAIL"

echo ""
echo "=========================================="
echo "ALL TESTS COMPLETE"
echo "=========================================="
```

Make it executable and run:
```bash
chmod +x test_all_scripts.sh
./test_all_scripts.sh
```

---

## Troubleshooting

### Error: "Connection not found"
```bash
# List configured connections
snow connection list

# Reconfigure connection
snow connection add --connection-name imcust ...
```

### Error: "Authentication failed"
```bash
# Test connection
snow connection test --connection imcust

# Verify PAT token is valid
echo $IMCUST_PAT  # Should not be empty

# Regenerate PAT in Snowflake UI if needed
```

### Error: "Object does not exist"
```bash
# Check current database/schema
snow sql --connection imcust --query "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();"

# Set correct database
snow sql --connection imcust --query "USE DATABASE PROD_DB;"
```

### Error: "SQL compilation error"
```bash
# Get detailed error message
snow sql --connection imcust --filename IMCUST/script.sql --verbose

# Check for:
# 1. Case sensitivity issues (use exact table names)
# 2. Missing semicolons
# 3. Incorrect quotes
```

---

## Common SnowCLI Commands

```bash
# Execute single query
snow sql --connection imcust --query "SELECT CURRENT_VERSION();"

# Execute SQL file
snow sql --connection imcust --filename script.sql

# Execute with output format
snow sql --connection imcust --query "SHOW TABLES;" --output json
snow sql --connection imcust --query "SHOW TABLES;" --output csv

# Execute with variables
snow sql --connection imcust --query "SELECT * FROM TABLE WHERE id = ?" --bind 123

# Get connection info
snow connection list
snow connection test --connection imcust

# Show SnowCLI version
snow --version

# Get help
snow sql --help
```

---

## Execution Order Summary

### Phase 1: IMCUST Setup
1. ✅ MANUAL_01_discovery.sql
2. ✅ MANUAL_02_extract_ddl.sql
3. ✅ MANUAL_03_create_share.sql
4. ✅ AUTOMATED_migration_procedure.sql (create procedures)

### Phase 2: IMSDLC Setup
1. ✅ MANUAL_01_consume_share.sql
2. ✅ AUTOMATED_migration_procedure.sql (create procedures)

### Phase 3: Migration Execution
1. ✅ Run SP_CREATE_TABLES_FROM_SHARE (or MANUAL_02)
2. ✅ Run SP_POPULATE_DATA_FROM_SHARE (or MANUAL_03)
3. ✅ MANUAL_04_validate.sql

### Phase 4: Cleanup
1. ⚠️ IMCUST/MANUAL_04_cleanup.sql (remove share)
2. ⚠️ IMSDLC/MANUAL_05_cleanup.sql (drop shared database)

---

## Status: ✅ ALL SCRIPTS READY FOR SNOWCLI TESTING

**Last Updated:** 2025-11-10
**Errors Fixed:**
- ✅ 3 RECORD type errors
- ✅ 4 DISTINCT in recursive CTE errors

**Next Action:** Configure SnowCLI connections and run tests
