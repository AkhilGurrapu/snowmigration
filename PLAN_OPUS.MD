# Snowflake Cross-Account Migration Plan: IMCUST → IMSDLC

## Executive Summary

This plan outlines a systematic approach to migrate selected database objects from the IMCUST production account (`prod_db`) to the IMSDLC development account (`dev_db`). The migration covers 5 tables, 1 view, and 2 stored procedures from the `mart_investments_bolt` and `src_investments_bolt` schemas.

## Migration Scope

### Source Account (IMCUST)

- **Database**: `prod_db`
- **Schemas**: `mart_investments_bolt`, `src_investments_bolt`
- **Objects to Migrate**:
- **SRC Schema**: 
- `stock_metadata_raw` (table)
- **MART Schema**: 
- `dim_stocks`, `dim_portfolios`, `fact_transactions`, `fact_daily_positions` (tables)
- `vw_current_holdings` (view)
- `sp_load_dim_stocks`, `sp_calculate_daily_positions` (stored procedures)

### Target Account (IMSDLC)

- **Database**: `dev_db` (already exists)
- **Schemas**: Same naming as source (already exist)
- **Requirement**: Create objects and migrate data

## Migration Architecture

### Phase 1: Discovery & Analysis

1. **Object Inventory** (`IMCUST/01_discovery.sql`)

- Query INFORMATION_SCHEMA for all objects
- Identify dependencies using OBJECT_DEPENDENCIES view
- Document row counts and data volumes

2. **Dependency Mapping** (`IMCUST/02_dependencies.sql`)

- Map view dependencies on tables
- Identify stored procedure dependencies
- Create dependency order for migration

### Phase 2: Data Share Setup

1. **Create Share** (`IMCUST/03_create_share.sql`)

- Create secure share in IMCUST account
- Grant usage on database and schemas
- Add all required tables to share

2. **Consume Share** (`IMSDLC/01_consume_share.sql`)

- Create database from share in IMSDLC
- Verify access to shared objects

### Phase 3: DDL Extraction & Transformation

1. **Extract DDL** (`IMCUST/04_extract_ddl.sql`)

- Use GET_DDL() for all objects
- Save DDL with fully qualified names

2. **Transform DDL** (`IMCUST/05_transform_ddl.py`)

- Python script to replace `prod_db` → `dev_db`
- Validate transformed DDL
- Generate executable SQL scripts

### Phase 4: Object Creation

1. **Create Tables** (`IMSDLC/02_create_tables.sql`)

- Create table structures (without data)
- Preserve clustering keys and constraints

2. **Create Views** (`IMSDLC/03_create_views.sql`)

- Create views with transformed references
- Validate view definitions

3. **Create Procedures** (`IMSDLC/04_create_procedures.sql`)

- Create stored procedures
- Update internal references

### Phase 5: Data Migration

1. **Populate Tables** (`IMSDLC/05_populate_data.sql`)

- Use CTAS from shared database
- Maintain clustering where applicable
- Track row counts for validation

### Phase 6: Validation & Testing

1. **Data Validation** (`IMSDLC/06_validate_data.sql`)

- Compare row counts
- Sample data comparison
- Check data types and nullability

2. **Object Testing** (`IMSDLC/07_test_objects.sql`)

- Test view functionality
- Execute stored procedures
- Verify results match source

### Phase 7: Final Verification

1. **Dependency Check** (`IMSDLC/08_verify_dependencies.sql`)

- Confirm all dependencies resolved
- Check for broken references
- Generate migration report

## Script Organization

### IMCUST Folder (Source Account)

- `01_discovery.sql` - Object inventory and analysis
- `02_dependencies.sql` - Dependency mapping
- `03_create_share.sql` - Data share creation
- `04_extract_ddl.sql` - DDL extraction
- `05_transform_ddl.py` - DDL transformation script

### IMSDLC Folder (Target Account)

- `01_consume_share.sql` - Consume data share
- `02_create_tables.sql` - Create table structures
- `03_create_views.sql` - Create views
- `04_create_procedures.sql` - Create stored procedures
- `05_populate_data.sql` - Data population via CTAS
- `06_validate_data.sql` - Data validation
- `07_test_objects.sql` - Functional testing
- `08_verify_dependencies.sql` - Final verification

## Execution Workflow

1. **Pre-Migration Checks**

- Verify connections to both accounts
- Confirm target schemas exist
- Check warehouse availability

2. **Execute Discovery Phase**

- Run IMCUST scripts 01-02
- Review discovered objects and dependencies
- Confirm migration scope

3. **Setup Data Sharing**

- Execute IMCUST/03_create_share.sql
- Execute IMSDLC/01_consume_share.sql
- Verify share accessibility

4. **Extract and Transform DDL**

- Run IMCUST/04_extract_ddl.sql
- Execute IMCUST/05_transform_ddl.py
- Review transformed DDL

5. **Create Objects in Target**

- Execute IMSDLC scripts 02-04 in order
- Verify object creation

6. **Migrate Data**

- Execute IMSDLC/05_populate_data.sql
- Monitor data transfer progress

7. **Validate and Test**

- Run IMSDLC scripts 06-07
- Document test results
- Address any issues

8. **Final Verification**

- Execute IMSDLC/08_verify_dependencies.sql
- Generate migration report
- Clean up temporary objects

## Success Criteria

1. All 5 tables created with correct structure
2. Data row counts match source exactly
3. View returns correct results
4. Stored procedures execute successfully
5. No broken dependencies
6. All tests pass validation

## Rollback Strategy

If issues occur:

1. Drop created objects in reverse dependency order
2. Remove consumed share
3. Document issues for resolution
4. Re-execute after fixes

## Future Process Template

This migration creates a reusable pattern for:

1. Discovery scripts for object inventory
2. Automated DDL extraction and transformation
3. Data share-based migration approach
4. Comprehensive validation framework
5. Test automation scripts

The scripts can be parameterized for future migrations between any accounts within the same Snowflake organization.