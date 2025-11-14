# Problem Statement: Missing Native Lineage on Target Side After Cross-Account Migration

## Context
We have a Snowflake cross-account migration framework that migrates tables and views from a source account (IMCUST) to a target account (IMSDLC) using data shares. The migration process:

1. **Source Side (IMCUST)**:
   - Discovers all upstream dependencies using `SNOWFLAKE.CORE.GET_LINEAGE()`
   - Generates DDL scripts for all objects
   - Generates CTAS scripts for data migration
   - Creates a data share with all objects
   - Shares metadata tables containing the scripts

2. **Target Side (IMSDLC)**:
   - Creates database from the share
   - Executes DDL scripts to create table structures
   - Executes CTAS scripts to populate data: `CREATE TABLE AS SELECT * FROM <SHARED_DB>.<schema>.<table>`

## The Problem

After migration, when querying native Snowflake lineage on the target side:

```sql
SELECT * FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE('DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS', 'TABLE', 'UPSTREAM'));
```

**Result: "No data"** - The lineage is completely missing.

## Root Cause Analysis

1. **CTAS from Shared Database Doesn't Create Lineage**: 
   - According to Snowflake documentation, `GET_LINEAGE()` does NOT work for objects in shared databases
   - When we do `CREATE TABLE AS SELECT * FROM <SHARED_DB>.<table>`, Snowflake doesn't establish lineage relationships because the source is a shared database object

2. **Current Implementation**:
   - All tables are migrated using CTAS from the shared database
   - This creates the data but doesn't establish any lineage relationships
   - The target tables appear as if they were created independently with no upstream dependencies

3. **What We Need**:
   - Native `GET_LINEAGE()` to work on target side, showing the same upstream dependencies as on source side
   - For example, `FACT_TRANSACTIONS` should show upstream dependencies to `TRANSACTIONS_RAW` and `BROKER_MASTER` (or their migrated equivalents)
   - The lineage should reflect the actual data flow, not just metadata dependencies

## Key Requirements

1. **Organic Lineage**: The lineage should be established "organically" - meaning it should be created the same way it would be on the source side, using the actual SELECT logic that was used to populate the table originally

2. **Preserve Original Logic**: 
   - On source side, `FACT_TRANSACTIONS` is populated with: `INSERT INTO fact_transactions SELECT ... FROM transactions_raw t JOIN broker_master b ...`
   - This creates natural lineage because it references actual source tables
   - On target side, we need to replicate this same SELECT logic, but referencing the migrated upstream tables

3. **No Direct INSERT from Shared DB**: 
   - We cannot do `INSERT INTO target_table SELECT * FROM shared_database.table` because:
     - It doesn't establish lineage (shared DB limitation)
     - It doesn't preserve the original transformation logic (joins, calculations, etc.)

## Example Scenario

**Source Side:**
- `FACT_TRANSACTIONS` is created with:
  ```sql
  INSERT INTO fact_transactions
  SELECT
      t.transaction_id as fact_transaction_id,
      t.transaction_id,
      t.stock_id as stock_key,
      t.broker_id as broker_key,
      ...
      ROUND(t.total_amount * b.commission_rate, 2) as commission_amount,
      CASE WHEN t.transaction_type = 'BUY' THEN ... END as net_amount
  FROM PROD_DB.SRC_INVESTMENTS_BOLT.transactions_raw t
  JOIN PROD_DB.SRC_INVESTMENTS_BOLT.broker_master b ON t.broker_id = b.broker_id;
  ```
- This creates lineage: `FACT_TRANSACTIONS` → `TRANSACTIONS_RAW` and `BROKER_MASTER`

**Target Side (Current - Broken):**
- `FACT_TRANSACTIONS` is created with:
  ```sql
  CREATE TABLE DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS AS
  SELECT * FROM IMCUST_SHARED_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS;
  ```
- This does NOT create lineage because source is a shared database

**Target Side (Desired - Working):**
- `FACT_TRANSACTIONS` should be created with:
  ```sql
  -- Table structure already exists from DDL
  INSERT INTO DEV_DB.MART_INVESTMENTS_BOLT.FACT_TRANSACTIONS
  SELECT
      t.transaction_id as fact_transaction_id,
      t.transaction_id,
      t.stock_id as stock_key,
      t.broker_id as broker_key,
      ...
      ROUND(t.total_amount * b.commission_rate, 2) as commission_amount,
      CASE WHEN t.transaction_type = 'BUY' THEN ... END as net_amount
  FROM DEV_DB.SRC_INVESTMENTS_BOLT.transactions_raw t
  JOIN DEV_DB.SRC_INVESTMENTS_BOLT.broker_master b ON t.broker_id = b.broker_id;
  ```
- This creates lineage: `FACT_TRANSACTIONS` → `TRANSACTIONS_RAW` and `BROKER_MASTER` (migrated versions)

## Technical Constraints

1. **Shared Database Limitation**: Snowflake documentation states: "Lineage is not available for objects in a shared database"

2. **INSERT ... SELECT Creates Lineage**: According to Snowflake docs, `INSERT ... SELECT` operations DO create lineage relationships when the source tables are regular (non-shared) tables

3. **Execution Order**: 
   - Base tables (no upstream deps) must be migrated first using CTAS from shared DB
   - Dependent tables must be migrated using INSERT ... SELECT from the already-migrated upstream tables
   - This creates a chain of lineage: base tables → dependent tables → more dependent tables

4. **Original SELECT Logic**: 
   - We need to extract or reconstruct the original SELECT statement that was used to populate each table on the source side
   - This includes joins, transformations, calculations, CASE statements, etc.
   - Then adapt it to reference the migrated table names on the target side

## Success Criteria

1. After migration, `GET_LINEAGE()` on target side returns the same upstream dependencies as on source side
2. The lineage shows the actual data flow through the migrated tables
3. The approach is "organic" - using the same SELECT logic that was used on source side
4. No direct INSERT from shared database (only from migrated upstream tables)

## Current State

- Migration framework is working for data migration
- All tables are successfully migrated with correct data
- DDL execution works correctly
- CTAS execution works correctly
- **BUT**: Native lineage is completely missing on target side
- `GET_LINEAGE()` returns "No data" for all migrated tables

