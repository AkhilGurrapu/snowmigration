# Snowflake Cross-Account Migration Scripts Index

## Overview
Complete SQL-based migration solution for IMCUST (PROD_DB) → IMSDLC (DEV_DB)

---

## IMCUST (Source Account) Scripts

### Manual Scripts
| File | Purpose | Execution Time |
|------|---------|----------------|
| `IMCUST/MANUAL_01_discovery.sql` | Discover all objects and dependencies using recursive CTEs | 15-30 min |
| `IMCUST/MANUAL_02_extract_ddl.sql` | Extract DDL for tables, views, procedures using GET_DDL | 10-20 min |
| `IMCUST/MANUAL_03_create_share.sql` | Create secure data share and grant access to IMSDLC | 5-10 min |
| `IMCUST/MANUAL_04_cleanup.sql` | Remove share after migration complete | 5 min |

### Automated Scripts
| File | Purpose |
|------|---------|
| `IMCUST/AUTOMATED_migration_procedure.sql` | Contains 3 stored procedures |

**Stored Procedures:**
1. `SP_PREPARE_MIGRATION_SHARE` - Discover dependencies, create share, add tables dynamically
2. `SP_EXTRACT_ALL_DDL` - Extract DDL for all object types
3. `SP_DISCOVER_DEPENDENCIES` - Recursive dependency discovery (upstream + downstream)

---

## IMSDLC (Target Account) Scripts

### Manual Scripts
| File | Purpose | Execution Time |
|------|---------|----------------|
| `IMSDLC/MANUAL_01_consume_share.sql` | Consume share from IMCUST and verify access | 5-10 min |
| `IMSDLC/MANUAL_02_create_objects.sql` | Create tables, views, procedures with transformed DDL | 20-30 min |
| `IMSDLC/MANUAL_03_populate_data.sql` | Migrate data using INSERT INTO SELECT from share | 1-4 hours |
| `IMSDLC/MANUAL_04_validate.sql` | Comprehensive validation (10 validation checks) | 30-45 min |
| `IMSDLC/MANUAL_05_cleanup.sql` | Drop temporary shared database | 5 min |

### Automated Scripts
| File | Purpose |
|------|---------|
| `IMSDLC/AUTOMATED_migration_procedure.sql` | Contains 6 stored procedures |

**Stored Procedures:**
1. `SP_TRANSFORM_DDL` - Transform DDL (PROD_DB → DEV_DB)
2. `SP_CREATE_TABLES_FROM_SHARE` - Auto-create tables from shared database
3. `SP_POPULATE_DATA_FROM_SHARE` - Auto-populate data with progress tracking
4. `SP_VALIDATE_MIGRATION` - Automated row count validation
5. `SP_COMPLETE_MIGRATION` - Complete workflow (create + populate + validate)
6. `SP_GENERATE_VIEW_PROCEDURE_DDL` - Helper for view/procedure transformation

---

## Execution Guide

| File | Purpose |
|------|---------|
| `EXECUTION_GUIDE.sql` | Complete execution instructions for both manual and automated approaches |

**Contains:**
- Part 1: Manual execution (9 steps)
- Part 2: Automated execution (6 steps)
- Part 3: Quick start guide
- Part 4: Verification queries
- Part 5: Troubleshooting
- Part 6: Cleanup procedures
- Part 7: Reusable templates

---

## Key Features

### Manual Approach
✅ Full control over each step
✅ Review DDL before execution
✅ Suitable for first-time migrations
✅ Educational - understand each phase

### Automated Approach
✅ Reusable for future migrations
✅ Dynamic object discovery
✅ Parameterized for any database/schema
✅ Built-in error handling
✅ Progress tracking and logging

### Technical Highlights
✅ **Recursive CTEs** for complete dependency discovery
✅ **GET_DDL** with fully qualified names
✅ **Dynamic SQL** with EXECUTE IMMEDIATE
✅ **Zero-copy architecture** via Secure Data Sharing
✅ **Comprehensive validation** (row counts, schemas, dependencies)

---

## Execution Paths

### Path 1: Manual (Full Control)
```
IMCUST: 01_discovery → 02_extract_ddl → 03_create_share
IMSDLC: 01_consume_share → 02_create_objects → 03_populate_data → 04_validate → 05_cleanup
IMCUST: 04_cleanup
```

### Path 2: Automated (Quick)
```
IMCUST: Install procedures → SP_PREPARE_MIGRATION_SHARE
IMSDLC: Install procedures → SP_COMPLETE_MIGRATION → SP_VALIDATE_MIGRATION → cleanup
IMCUST: cleanup
```

### Path 3: Hybrid (Recommended)
```
IMCUST: SP_DISCOVER_DEPENDENCIES (review) → SP_PREPARE_MIGRATION_SHARE
IMSDLC: SP_CREATE_TABLES_FROM_SHARE → SP_POPULATE_DATA_FROM_SHARE → Manual view/procedure creation → SP_VALIDATE_MIGRATION
```

---

## Migration Checklist

### Pre-Migration
- [ ] PAT tokens configured
- [ ] ACCOUNTADMIN access verified
- [ ] ADMIN_WH warehouse available
- [ ] DEV_DB and schemas exist
- [ ] Wait 3 hours after object changes (ACCOUNT_USAGE latency)

### Migration Execution
- [ ] Discovery completed
- [ ] All dependencies identified
- [ ] Share created successfully
- [ ] Share consumed in IMSDLC
- [ ] Tables created
- [ ] Data populated
- [ ] Views created
- [ ] Procedures created
- [ ] All validations PASS

### Post-Migration
- [ ] Row counts match 100%
- [ ] Views queryable
- [ ] Procedures executable
- [ ] No broken dependencies
- [ ] Temporary objects cleaned up
- [ ] Documentation updated

---

## Success Criteria

| Validation | Target | Critical |
|------------|--------|----------|
| Row count match | 100% | YES |
| Schema definition match | 100% | YES |
| Views queryable | 100% | YES |
| Procedures executable | 100% | YES |
| Dependencies resolved | 100% | YES |
| External dependencies | 0 | YES |
| Data type match | 100% | YES |

---

## Files Summary

**Total Files Created:** 11
**Manual Scripts:** 9
**Automated Scripts:** 2
**Execution Guide:** 1
**Index:** 1

**Lines of Code:** ~2,500+ SQL statements
**Stored Procedures:** 9 total (3 IMCUST + 6 IMSDLC)
**Validation Checks:** 10 comprehensive validations

---

## Quick Reference Commands

### IMCUST
```sql
-- Automated share creation
CALL PROD_DB.PUBLIC.SP_PREPARE_MIGRATION_SHARE(
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT',
    'STOCK_METADATA_RAW,DIM_STOCKS,DIM_PORTFOLIOS,FACT_TRANSACTIONS,FACT_DAILY_POSITIONS',
    'MIGRATION_SHARE_IMCUST_TO_IMSDLC',
    'nfmyizv.imsdlc'
);
```

### IMSDLC
```sql
-- Automated migration
CALL DEV_DB.PUBLIC.SP_COMPLETE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);

-- Validation
CALL DEV_DB.PUBLIC.SP_VALIDATE_MIGRATION(
    'MIGRATION_SHARED_DB',
    'DEV_DB',
    'SRC_INVESTMENTS_BOLT,MART_INVESTMENTS_BOLT'
);
```

---

## Best Practices

1. **Always run discovery first** to understand full scope
2. **Review dependencies** before creating share
3. **Transform DDL carefully** for views and procedures
4. **Validate after each major step**
5. **Keep shared database** for 7 days post-migration
6. **Document any deviations** from plan
7. **Test procedures** before using in production

---

## Next Steps

1. Review EXECUTION_GUIDE.sql for detailed instructions
2. Choose manual, automated, or hybrid approach
3. Execute pre-migration checklist
4. Run discovery in IMCUST
5. Review and approve migration scope
6. Execute migration
7. Validate results
8. Cleanup temporary objects
