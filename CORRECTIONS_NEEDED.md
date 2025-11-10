# SQL Scripts Corrections Summary

## Column Name Corrections Applied

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

### Files Still Need Correction:
2. IMCUST/AUTOMATED_migration_procedure.sql - OBJECT_DEPENDENCIES columns
3. IMSDLC/MANUAL_04_validate.sql - OBJECT_DEPENDENCIES columns
4. All procedures using SPLIT_TO_TABLE - need to use VALUE column (not just direct access)

### SPLIT_TO_TABLE Return Columns:
- SEQ - Sequence number
- INDEX - Element index (1-based)
- VALUE - The actual value

### Correct Usage:
```sql
SELECT VALUE
FROM TABLE(SPLIT_TO_TABLE('schema1,schema2', ','))
```

## Next Steps:
1. Fix AUTOMATED_migration_procedure.sql for both IMCUST and IMSDLC
2. Fix validation scripts
3. Update execution guide
4. Test all scripts for syntax
