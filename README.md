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
✅ **Cross-schema support** - handles dependencies across multiple schemas (MART_INVESTMENTS_BOLT, SRC_INVESTMENTS_BOLT, etc.)
✅ **Complete object coverage** - always includes requested objects, even if they have no dependencies
✅ **Correct execution ordering** - DDL scripts execute based on DISTANCE field from GET_LINEAGE (deepest dependencies first)
✅ **Database name replacement** (prod_db → dev_db) while preserving schema names
✅ **Schema preservation** - objects created in correct target schemas matching source structure
✅ **Data sharing with database roles** (Snowflake best practice)
✅ **Complete automation** - single procedure call on each side
✅ **Error handling** - continues execution and logs failures
✅ **Execution tracking** - complete audit trail with dependency level tracking
✅ **Object type detection** - automatically detects TABLE vs VIEW
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

### Stored Procedure Syntax
- **JavaScript Stored Procedures** (IMCUST): Use FLOAT instead of NUMBER for parameters
- **SQL Stored Procedures** (IMSDLC): Use RESULTSET pattern for dynamic SQL with cursors
- **String Concatenation**: Use || operator but prepare strings before USING clause

### Metadata and Tracking
- **Data Sharing**: Includes migration metadata tables for target-side automation
- **Dependency Levels**: Level 0 = requested objects, Level 1+ = upstream dependencies
- **Schema Information**: Captured from `SOURCE_OBJECT_DATABASE` and `SOURCE_OBJECT_SCHEMA` in GET_LINEAGE
- **Object Type Detection**: Queries `INFORMATION_SCHEMA.VIEWS` to determine TABLE vs VIEW

### TABLE vs VIEW Handling

**Key Difference**: VIEWs do NOT require data population (CTAS), only DDL execution.

#### Processing Flow

| Step | TABLEs | VIEWs |
|------|--------|-------|
| **1. Dependency Discovery** | ✅ Captured by GET_LINEAGE | ✅ Captured by GET_LINEAGE |
| **2. DDL Extraction** | ✅ GET_DDL('TABLE', ...) | ✅ GET_DDL('VIEW', ...) |
| **3. CTAS Generation** | ✅ Generated (line 93 check) | ❌ **Skipped** (`if obj_type === 'TABLE'`) |
| **4. Share Access** | ✅ Granted for CTAS source | ✅ Granted for reference |
| **5. Target DDL** | ✅ Creates empty table | ✅ Creates view with logic |
| **6. Target CTAS** | ✅ Populates with data | ❌ **Skipped** (no CTAS exists) |

#### Example: Mixed TABLE and VIEW Migration

```sql
-- Request migration of a VIEW that depends on TABLEs
CALL sp_orchestrate_migration(
    'PROD_DB',
    'MART_INVESTMENTS_BOLT',
    'DEV_DB',
    'MART_INVESTMENTS_BOLT',
    ARRAY_CONSTRUCT('VW_TRANSACTION_ANALYSIS'),  -- This is a VIEW
    'MIGRATION_SHARE_001',
    'IMSDLC'
);
```

**Generated Metadata**:

```
migration_share_objects (3 objects):
├── VW_TRANSACTION_ANALYSIS (VIEW, level=0)     ← Requested view
├── FACT_TRANSACTIONS (TABLE, level=1)          ← Dependency
└── DIM_STOCKS (TABLE, level=1)                 ← Dependency

migration_ddl_scripts (3 scripts):
├── VW_TRANSACTION_ANALYSIS (VIEW DDL)
├── FACT_TRANSACTIONS (TABLE DDL)
└── DIM_STOCKS (TABLE DDL)

migration_ctas_scripts (2 scripts ONLY):
├── FACT_TRANSACTIONS (CTAS)                    ← TABLE needs data
└── DIM_STOCKS (CTAS)                           ← TABLE needs data
    (VW_TRANSACTION_ANALYSIS NOT INCLUDED!)     ← VIEW doesn't need CTAS
```

**Target Execution**:

```sql
-- Step 1: sp_execute_target_ddl creates all structures
CREATE TABLE DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS (...);  -- Empty table
CREATE TABLE DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS (...);         -- Empty table
CREATE VIEW DEV_DB.MART_INVESTMENTS_BOLT.VW_TRANSACTION_ANALYSIS AS -- View with logic
    SELECT t.*, s.stock_name
    FROM DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS t
    JOIN DEV_DB.MART_INVESTMENTS_BOLT.DIM_STOCKS s ON t.stock_id = s.stock_id;

-- Step 2: sp_execute_target_ctas populates ONLY tables
CREATE OR REPLACE TABLE ... FACT_TRANSACTIONS AS SELECT * FROM SHARED...;
CREATE OR REPLACE TABLE ... DIM_STOCKS AS SELECT * FROM SHARED...;
-- VW_TRANSACTION_ANALYSIS skipped - already functional after DDL!
```

**Result**: VIEW works immediately after DDL, querying the newly populated TABLEs.

### Warehouse Sizing Requirements

#### Source Account (IMCUST) - Lightweight Operations

**Operations**:
- GET_LINEAGE queries (metadata only)
- GET_DDL extraction (metadata only)
- Metadata table insertions
- Share and permission creation

**Recommended Warehouse**: `XSMALL` or `SMALL`

```sql
-- Source warehouse - minimal compute needed
USE WAREHOUSE ADMIN_WH;  -- XSMALL sufficient
CALL sp_orchestrate_migration(...);  -- Completes in seconds
```

**Typical Execution**: 1-3 minutes, ~0.03 credits

#### Target Account (IMSDLC) - Data-Intensive Operations

**Operations**:
- DDL execution (fast, metadata only)
- **CTAS execution (data-intensive!)**: Full table scans + writes for ALL tables

**Recommended Warehouse**: `MEDIUM` to `LARGE` (or larger for big datasets)

```sql
-- Target warehouse - sized for data volume
USE WAREHOUSE MIGRATION_WH;
ALTER WAREHOUSE MIGRATION_WH SET WAREHOUSE_SIZE = 'LARGE';

CALL sp_execute_full_migration(...);  -- Data copying happens here
```

**Warehouse Sizing Guide**:

| Data Volume | Warehouse Size | Estimated Cost | Typical Duration |
|-------------|----------------|----------------|------------------|
| < 1 GB | SMALL | ~0.1 credits | 2-5 minutes |
| 1-50 GB | MEDIUM | ~0.5 credits | 5-15 minutes |
| 50-200 GB | LARGE | ~2-3 credits | 15-30 minutes |
| 200-500 GB | XLARGE | ~5-8 credits | 30-60 minutes |
| > 500 GB | XXLARGE+ | Varies | 1+ hours |

**Cost Breakdown Example** (50 GB migration):

| Account | Operation | Warehouse | Time | Credits |
|---------|-----------|-----------|------|---------|
| IMCUST | Metadata extraction | XSMALL | 2 min | 0.03 |
| IMSDLC | DDL execution | MEDIUM | 1 min | 0.07 |
| IMSDLC | **CTAS (data copy)** | LARGE | 20 min | **2.67** |
| | | | **Total** | **~2.77** |

**Key Insight**: 95%+ of compute cost is in target-side CTAS operations!

**Optimization Tips**:

```sql
-- 1. Use auto-suspend on source (operations are quick)
ALTER WAREHOUSE ADMIN_WH SET AUTO_SUSPEND = 60;  -- 1 minute

-- 2. Size up target warehouse for migration window
ALTER WAREHOUSE MIGRATION_WH SET
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 300;  -- 5 minutes (migration takes time)

-- 3. Run migration
CALL sp_execute_full_migration(...);

-- 4. Manually suspend after completion
ALTER WAREHOUSE MIGRATION_WH SUSPEND;
```

## Recent Fixes

### Fix 1: Cross-Schema Dependencies (2025-11-10)
- ✅ System now captures `SOURCE_OBJECT_SCHEMA` from GET_LINEAGE
- ✅ Objects created in correct target schemas (e.g., SRC_INVESTMENTS_BOLT → SRC_INVESTMENTS_BOLT)
- ✅ CTAS scripts preserve schema mapping
- ✅ Share grants include USAGE on all involved schemas

### Fix 2: Requested Objects Always Included (2025-11-11)
- ✅ Objects with zero dependencies are now included in migration
- ✅ Requested objects explicitly added with `dependency_level = 0`
- ✅ Automatic TABLE/VIEW type detection
- ✅ Enhanced reporting shows breakdown of requested vs. dependent objects

**See [CROSS_SCHEMA_FIX_SUMMARY.md](CROSS_SCHEMA_FIX_SUMMARY.md) for complete details**

## Future Enhancements

- Add row count validation
- Support for procedures and functions (currently tables/views only)
- Parallel execution for independent objects
- Rollback capability
