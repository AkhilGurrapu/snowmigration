# Snowflake Cross-Account Migration Automation

## Overview
Automated Snowflake cross-account migration solution for migrating objects from `IMCUST` (prod_db) to `IMSDLC` (dev_db) with complete dependency resolution using data shares and stored procedures.

## Folder Structure

```
snowmigration/
├── IMCUST/                          # Source account (IMCUST) scripts
│   ├── 01_setup_config_tables.sql   # Create migration metadata tables
│   ├── 02_sp_get_upstream_dependencies.sql   # Find upstream dependencies
│   ├── 03_sp_generate_migration_scripts.sql  # Generate DDL and CTAS scripts
│   ├── 04_sp_setup_data_share.sql   # Create data share with database role
│   ├── 05_sp_orchestrate_migration.sql       # Main orchestrator procedure
│   └── 99_example_execution.sql     # Example usage
├── IMSDLC/                          # Target account (IMSDLC) scripts
│   ├── 01_setup_execution_log.sql   # Create execution log table
│   ├── 02_sp_execute_target_ddl.sql # Execute DDL scripts
│   ├── 03_sp_execute_target_ctas.sql        # Execute CTAS scripts
│   ├── 04_sp_execute_full_migration.sql     # Main orchestrator procedure
│   ├── 05_sp_validate_migration.sql         # Validation procedure
│   └── 99_example_execution.sql     # Example usage
├── config/                          # Connection configurations
│   ├── connections.toml             # VS Code Snowflake extension config
│   ├── imcust.yaml                  # IMCUST connection config
│   └── imsdlc.yaml                  # IMSDLC connection config
├── .env                             # Environment variables (PAT tokens)
├── CLAUDE.md                        # Project instructions
└── plan.md                          # Migration plan details
```

## Validation Status

### IMCUST (Source Account) - ✅ All Scripts Validated
- Config tables created successfully
- 4 stored procedures created successfully:
  - sp_get_upstream_dependencies
  - sp_generate_migration_scripts
  - sp_setup_data_share
  - sp_orchestrate_migration

### IMSDLC (Target Account) - ✅ All Scripts Validated
- Execution log table created successfully
- 4 stored procedures created successfully:
  - sp_execute_target_ddl
  - sp_execute_target_ctas
  - sp_execute_full_migration
  - sp_validate_migration

## How to Use

### Step 1: Setup Source Account (IMCUST)

Execute scripts in order:

```bash
# Connect to IMCUST account
snow sql --connection imcust --filename IMCUST/01_setup_config_tables.sql
snow sql --connection imcust --filename IMCUST/02_sp_get_upstream_dependencies.sql
snow sql --connection imcust --filename IMCUST/03_sp_generate_migration_scripts.sql
snow sql --connection imcust --filename IMCUST/04_sp_setup_data_share.sql
snow sql --connection imcust --filename IMCUST/05_sp_orchestrate_migration.sql
```

### Step 2: Execute Migration (IMCUST)

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE prod_db;
USE SCHEMA mart_investments_bolt;

-- Run the migration orchestration
CALL sp_orchestrate_migration(
    'PROD_DB',                                    -- source database
    'MART_INVESTMENTS_BOLT',                      -- source schema
    'DEV_DB',                                     -- target database
    'MART_INVESTMENTS_BOLT',                      -- target schema
    ARRAY_CONSTRUCT('TABLE1', 'TABLE2', 'VIEW1'), -- objects to migrate
    'MIGRATION_SHARE_001'                         -- share name
);
```

This will:
1. Find all upstream dependencies using SNOWFLAKE.CORE.GET_LINEAGE
2. Extract DDLs for all objects
3. Replace database names (prod_db → dev_db)
4. Create database role and grant SELECT on all dependency objects
5. Create data share and add target account
6. Store all scripts in metadata tables

### Step 3: Setup Target Account (IMSDLC)

Execute scripts in order:

```bash
# Connect to IMSDLC account
snow sql --connection imsdlc --filename IMSDLC/01_setup_execution_log.sql
snow sql --connection imsdlc --filename IMSDLC/02_sp_execute_target_ddl.sql
snow sql --connection imsdlc --filename IMSDLC/03_sp_execute_target_ctas.sql
snow sql --connection imsdlc --filename IMSDLC/04_sp_execute_full_migration.sql
snow sql --connection imsdlc --filename IMSDLC/05_sp_validate_migration.sql
```

### Step 4: Execute Migration (IMSDLC)

```sql
USE ROLE ACCOUNTADMIN;

-- Create database from share
CREATE DATABASE IF NOT EXISTS shared_prod_db
FROM SHARE IMCUST.MIGRATION_SHARE_001;

GRANT IMPORTED PRIVILEGES ON DATABASE shared_prod_db TO ROLE ACCOUNTADMIN;

-- Execute complete migration (DDL + CTAS)
CALL dev_db.mart_investments_bolt.sp_execute_full_migration(
    1,                      -- migration_id from source
    'shared_prod_db',       -- shared database name
    TRUE                    -- validate before CTAS
);
```

This will:
1. Read DDL scripts from shared database
2. Execute DDLs in dependency order
3. Execute CTAS scripts to copy data
4. Log all execution results
5. Handle errors gracefully

## Key Features

✅ **Automatic dependency resolution** using SNOWFLAKE.CORE.GET_LINEAGE
✅ **Database name replacement** (prod_db → dev_db) while preserving schema names
✅ **Data sharing with database roles** (Snowflake best practice)
✅ **Complete automation** - single procedure call on each side
✅ **Error handling** - continues execution and logs failures
✅ **Execution tracking** - complete audit trail
✅ **Syntax validated** - all scripts tested with Snow CLI

## Monitoring & Validation

### View Migration Status (IMCUST)

```sql
-- View migration config
SELECT * FROM migration_config ORDER BY migration_id DESC;

-- View generated DDL scripts
SELECT object_name, object_type, dependency_level
FROM migration_ddl_scripts WHERE migration_id = 1
ORDER BY dependency_level, object_name;

-- View CTAS scripts
SELECT object_name, execution_order
FROM migration_ctas_scripts WHERE migration_id = 1
ORDER BY execution_order;

-- View shared objects
SELECT * FROM migration_share_objects WHERE migration_id = 1;
```

### View Execution Results (IMSDLC)

```sql
-- View execution log
SELECT execution_phase, object_name, status, error_message, execution_time_ms
FROM migration_execution_log WHERE migration_id = 1
ORDER BY log_id;

-- Summary statistics
SELECT execution_phase, status, COUNT(*) as count,
       AVG(execution_time_ms) as avg_time_ms
FROM migration_execution_log WHERE migration_id = 1
GROUP BY execution_phase, status;

-- View failures only
SELECT * FROM migration_execution_log
WHERE migration_id = 1 AND status = 'FAILED';
```

## Technical Notes

- **JavaScript Stored Procedures** (IMCUST): Use FLOAT instead of NUMBER for parameters
- **SQL Stored Procedures** (IMSDLC): Use RESULTSET pattern for dynamic SQL with cursors
- **String Concatenation**: Use || operator but prepare strings before USING clause
- **Data Sharing**: Includes migration metadata tables for target-side automation

## Future Enhancements

- Add row count validation
- Support for procedures and functions (currently tables/views only)
- Parallel execution for independent objects
- Rollback capability
- Cross-schema migrations
