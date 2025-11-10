# Verified Facts from Snowflake Documentation
**Snowflake Cross-Account Migration: IMCUST → IMSDLC**

This document summarizes key facts verified through official Snowflake documentation research using the MCP Snowflake Documentation server.

---

## 1. Data Shares: Read-Only Nature ✅ CONFIRMED

### Official Documentation Quote:
> "All database objects shared between accounts are read-only (i.e. the objects cannot be modified or deleted, including adding or modifying table data)."

**Source**: Snowflake Documentation - "About Secure Data Sharing"

### Key Facts:
1. **Zero-Copy Architecture**:
   - No data is copied or transferred between accounts
   - All sharing uses Snowflake's services layer and metadata store
   - Shared data does not take up storage in consumer account
   - No storage charges for consumer account

2. **Performance**:
   - Near-instantaneous access (no data movement latency)
   - Setup is quick and easy for providers
   - Access is immediate for consumers

3. **Cost Efficiency** (Same Organization/Region):
   - No egress costs between accounts in same region
   - Only charges are for compute resources (warehouses) used to query data
   - Consumer does not pay for storage

4. **Limitations**:
   - Cannot modify shared objects
   - Cannot delete shared objects
   - Cannot add or modify table data in shared objects
   - Cannot execute stored procedures that write to shared database
   - Cannot share mutable metadata coordination tables

### Implications for Migration:
✅ **CORRECT APPROACH**: Use data shares for cross-account data access
✅ **ENHANCEMENT NEEDED**: Use CTAS or INSERT INTO SELECT to populate target tables
❌ **INVALID**: Cannot use shares for bi-directional metadata coordination

---

## 2. Object Dependencies Discovery ✅ CONFIRMED WITH LIMITATIONS

### Official Documentation:
> "This Account Usage view displays object dependencies. An object dependency results when an object references a base object but does not materialize or copy data, such as when a view references a table."

**Source**: Snowflake Documentation - "OBJECT_DEPENDENCIES view"

### Available Views:
1. **SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES**
   - Latency: Up to 3 hours
   - Tracks: VIEW → TABLE, UDF → TABLE, PROCEDURE → TABLE dependencies

2. **SNOWFLAKE.ORGANIZATION_USAGE.OBJECT_DEPENDENCIES**
   - Organization-level view (Premium feature)
   - Cross-account visibility

### Tracked Dependencies:
- Views depending on tables
- Materialized views depending on tables
- UDFs (SQL) depending on tables
- Stored procedures depending on tables
- Dynamic tables depending on tables

### Dependency Types:
- **BY_NAME**: Dependency by object name only
- **BY_ID**: Dependency by object ID only
- **BY_NAME_AND_ID**: Combined dependency

### Important Limitations:

1. **Session Parameters**:
   > "Snowflake cannot accurately compute the dependencies of objects that include session parameters in their definitions because session parameters can take on different values depending on the context."
   - Recommendation: Avoid using session variables in view and function definitions

2. **Object Resolution**:
   > "If a view definition uses a function to call an object to create the view, or if an object is called inside another function or view, Snowflake does not record an object dependency."
   - Example: `get_presigned_url(@stage1, 'data.csv')` - dependency on stage1 is NOT recorded

3. **Broken Dependencies**:
   > "If the dependency type value is BY_NAME_AND_ID and an object dependency changes due to a CREATE OR REPLACE or ALTER operation on an object, Snowflake only records the object dependency prior to these operations."
   - After CREATE OR REPLACE, broken references are not recorded

4. **Data Movement ≠ Dependency**:
   > "Data movement, such as when data is copied or materialized from one object to another, does not result in an object dependency. For example, CREATE TABLE AS SELECT (CTAS), INSERT, or MERGE operations on tables result in data movement and are not included in this view."

5. **Data Sharing Limitations**:
   - Provider accounts cannot see dependent objects in consumer accounts
   - Consumer accounts cannot see dependent objects in provider accounts

### Implications for Migration:
✅ **USE**: OBJECT_DEPENDENCIES for initial discovery
⚠️ **ENHANCE**: Combine with INFORMATION_SCHEMA queries
⚠️ **VALIDATE**: Manual review of stored procedures for dynamic dependencies
⚠️ **CONSIDER**: 3-hour latency when timing migrations

---

## 3. DDL Extraction ✅ CONFIRMED

### Official Documentation:
> "Returns a DDL statement that can be used to recreate the specified object. For databases and schemas, GET_DDL is recursive (that is, it returns the DDL statements for recreating all supported objects within the specified database/schema)."

**Source**: Snowflake Documentation - "GET_DDL Function"

### Syntax:
```sql
GET_DDL('<object_type>', '[<namespace>.]<object_name>', <use_fully_qualified_names>)
```

### Supported Object Types:
- CONTACT
- DATABASE (recursive)
- DYNAMIC_TABLE
- EVENT_TABLE
- FILE_FORMAT
- FUNCTION (UDFs, including data metric functions and external functions)
- ICEBERG_TABLE
- INTEGRATION (storage)
- PIPE
- POLICY (aggregation, authentication, join, masking, password, projection, row access, session, storage lifecycle)
- **PROCEDURE** (stored procedures) ✅
- SCHEMA (recursive)
- SEMANTIC_VIEW
- SEQUENCE
- STREAM
- **TABLE** (tables, external tables, hybrid tables) ✅
- TAG (object tagging)
- TASK
- **VIEW** (views and materialized views) ✅
- WAREHOUSE

### Key Features:

1. **Fully Qualified Names**:
   - Optional parameter: `use_fully_qualified_names_for_recreated_objects`
   - When TRUE: Returns fully qualified names (database.schema.object)
   - When FALSE (default): Returns relative names

2. **Recursive for Databases/Schemas**:
   - Returns DDL for all contained objects
   - Useful for bulk extraction

3. **Tag and Policy Metadata**:
   - Includes tag assignments in CREATE OR REPLACE statements
   - Includes masking policies, row access policies, storage lifecycle policies
   - Tags sorted alphabetically by tag name

4. **Collation Information**:
   - Collation details are included in output

### Example Usage:
```sql
-- Get DDL for a table with fully qualified names
SELECT GET_DDL('TABLE', 'prod_db.mart_investments_bolt.table_name', TRUE);

-- Get DDL for a view
SELECT GET_DDL('VIEW', 'prod_db.mart_investments_bolt.view_name', TRUE);

-- Get DDL for a stored procedure (include signature)
SELECT GET_DDL('PROCEDURE', 'prod_db.mart_investments_bolt.proc_name(varchar)', TRUE);

-- Get DDL for entire schema (recursive)
SELECT GET_DDL('SCHEMA', 'prod_db.mart_investments_bolt', TRUE);
```

### Implications for Migration:
✅ **USE**: GET_DDL with `use_fully_qualified_names=TRUE` for accurate extraction
✅ **BENEFIT**: Single function call per object type
✅ **TRANSFORM**: String replacement of qualified names (prod_db → dev_db)
⚠️ **VALIDATE**: Ensure no dynamic references remain after transformation

---

## 4. INFORMATION_SCHEMA Views ✅ CONFIRMED

### Official Documentation:
> "The Snowflake Information Schema (aka 'Data Dictionary') consists of a set of system-defined views and table functions that provide extensive metadata information about the objects created in your account."

**Source**: Snowflake Documentation - "Snowflake Information Schema"

### Key Views Available:

#### 4.1 TABLES View
- **Purpose**: Displays all tables and views in database
- **Key Columns**:
  - TABLE_CATALOG (database name)
  - TABLE_SCHEMA (schema name)
  - TABLE_NAME
  - TABLE_OWNER
  - TABLE_TYPE (BASE TABLE, TEMPORARY TABLE, EXTERNAL TABLE, VIEW, MATERIALIZED VIEW)
  - ROW_COUNT
  - BYTES
  - RETENTION_TIME
  - CREATED, LAST_ALTERED, LAST_DDL timestamps

#### 4.2 COLUMNS View
- **Purpose**: Column-level metadata
- **Key Columns**:
  - TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
  - COLUMN_NAME
  - ORDINAL_POSITION
  - DATA_TYPE
  - IS_NULLABLE
  - COLUMN_DEFAULT
  - CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE

#### 4.3 VIEWS View
- **Purpose**: View definitions
- **Key Columns**:
  - TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
  - TABLE_OWNER
  - VIEW_DEFINITION (query expression)
  - IS_SECURE
  - CREATED, LAST_ALTERED, LAST_DDL timestamps

#### 4.4 PROCEDURES View
- **Purpose**: Stored procedure metadata
- **Key Columns**:
  - PROCEDURE_CATALOG, PROCEDURE_SCHEMA, PROCEDURE_NAME
  - PROCEDURE_OWNER
  - ARGUMENT_SIGNATURE (type signature of arguments)
  - DATA_TYPE (return value data type)
  - PROCEDURE_LANGUAGE (JAVASCRIPT, SQL, PYTHON, etc.)
  - PROCEDURE_DEFINITION (definition text)
  - CREATED, LAST_ALTERED timestamps

#### 4.5 Other Relevant Views
- **FUNCTIONS**: UDF metadata
- **SEQUENCES**: Sequence definitions
- **FILE_FORMATS**: File format definitions
- **SCHEMATA**: Schema-level metadata
- **ELEMENT_TYPES**: For structured ARRAY types
- **FIELDS**: For structured OBJECT and MAP types

### Query Examples:
```sql
-- Get all tables in target schemas
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES
FROM prod_db.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('MART_INVESTMENTS_BOLT', 'SRC_INVESTMENTS_BOLT')
  AND TABLE_TYPE = 'BASE TABLE';

-- Get all columns for a table
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
FROM prod_db.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MART_INVESTMENTS_BOLT'
  AND TABLE_NAME = 'target_table'
ORDER BY ORDINAL_POSITION;

-- Get all views with definitions
SELECT TABLE_NAME, VIEW_DEFINITION, IS_SECURE
FROM prod_db.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'MART_INVESTMENTS_BOLT';

-- Get all stored procedures with signatures
SELECT PROCEDURE_NAME, ARGUMENT_SIGNATURE, DATA_TYPE, PROCEDURE_LANGUAGE
FROM prod_db.INFORMATION_SCHEMA.PROCEDURES
WHERE PROCEDURE_SCHEMA = 'MART_INVESTMENTS_BOLT';
```

### Implications for Migration:
✅ **USE**: INFORMATION_SCHEMA for comprehensive object inventory
✅ **BENEFIT**: Real-time data (no latency like ACCOUNT_USAGE)
✅ **COMBINE**: With GET_DDL for complete metadata extraction
✅ **VALIDATE**: Row counts and checksums after migration

---

## 5. CREATE TABLE AS SELECT (CTAS) ✅ CONFIRMED

### Official Documentation:
> "Creates a new table populated with the data returned by a query"

**Source**: Snowflake Documentation - "CREATE TABLE AS SELECT"

### Syntax:
```sql
CREATE [ OR REPLACE ] TABLE <table_name> [ ( <col_name> [ <col_type> ] , ... ) ]
  [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
  [ COPY GRANTS ]
  [ ... ]
  AS <query>
```

### Key Features:

1. **Data Population**:
   - Creates table and populates in single operation
   - Can query from shared databases
   - Efficient for large data volumes

2. **Clustering Keys**:
   - Can specify clustering keys in CTAS
   - Automatic clustering enabled by default
   - Data is clustered when table is created
   - Note: Clustering adds sort operation (takes longer)

3. **Column Definitions**:
   - Optional: Can specify column names only
   - Types inferred from query
   - Must match number of SELECT list items

4. **Masking Policies**:
   - Can apply masking policies in CTAS
   - Policies applied BEFORE data is populated
   - Masked data stored in new table

5. **COPY GRANTS**:
   - Valid only with OR REPLACE clause
   - Copies grants from table being replaced (not from source)

### Usage with Data Shares:
```sql
-- Create table from shared data
CREATE TABLE dev_db.mart_investments_bolt.table_name AS
SELECT * FROM imcust_shared_db.mart_investments_bolt.source_table;

-- With clustering
CREATE TABLE dev_db.mart_investments_bolt.table_name
  CLUSTER BY (date_column)
AS
SELECT * FROM imcust_shared_db.mart_investments_bolt.source_table;

-- Alternative: INSERT INTO SELECT
INSERT INTO dev_db.mart_investments_bolt.existing_table
SELECT * FROM imcust_shared_db.mart_investments_bolt.source_table;
```

### Performance Considerations:
- CTAS is efficient for large data volumes
- Zero-copy from share means no intermediate staging
- Clustering adds overhead but improves query performance
- Use warehouse appropriate for data volume

### Implications for Migration:
✅ **PRIMARY METHOD**: Use CTAS for data population from shares
✅ **EFFICIENCY**: Single operation for table creation + data load
✅ **ALTERNATIVE**: INSERT INTO SELECT for existing tables
⚠️ **CONSIDER**: Clustering overhead vs. query performance benefits

---

## 6. Programmatic Access Token (PAT) Authentication ✅ CONFIRMED

### Official Documentation:
> "Programmatic access token (PAT) is a Snowflake-specific authentication method. The feature must be enabled for the account before usage. Authentication with PAT doesn't involve any human interaction."

**Source**: Snowflake Documentation - "Authenticating with a programmatic access token (PAT)"

### Connection Method:
```python
import snowflake.connector

conn = snowflake.connector.connect(
    user='svc4snowflakedeploy',
    password=pat_token,  # PAT token as password parameter
    account='nfmyizv-imcust',
    warehouse='admin_wh',
    database='prod_db',
    role='ACCOUNTADMIN'
)
```

### REST API Method:
```bash
curl --location 'https://myorganization-myaccount.snowflakecomputing.com/api/v2/databases' \
  --header "Authorization: Bearer <token_secret>"
```

### Key Features:
1. **No Human Interaction**: Fully automated authentication
2. **Secure**: Token-based, can be rotated
3. **Account-Level**: Must be enabled at account level
4. **Service Accounts**: Ideal for svc4snowflakedeploy user

### Connection Status:
✅ **IMCUST**: Connected successfully (UCB21816)
✅ **IMSDLC**: Connected successfully (FMB48463)
✅ **ADMIN**: Connected successfully (JMB03531)

### Implications for Migration:
✅ **AUTHENTICATION METHOD**: PAT is working for all accounts
✅ **AUTOMATION READY**: Can build Python orchestration
✅ **SECURE**: Tokens stored in .env files (not in code)

---

## 7. Summary: Migration Architecture Validation

### ✅ Confirmed Approaches:

1. **Data Shares for Cross-Account Access**:
   - Use shares to expose source data to target account
   - Zero-copy, no storage costs, immediate access
   - Read-only limitation requires CTAS/INSERT for data population

2. **Comprehensive Object Discovery**:
   - Combine INFORMATION_SCHEMA + OBJECT_DEPENDENCIES + GET_DDL
   - Account for OBJECT_DEPENDENCIES limitations
   - Manual review for dynamic dependencies in stored procedures

3. **DDL Transformation**:
   - Use GET_DDL with fully qualified names
   - String replacement: prod_db → dev_db
   - Validate transformed DDL before execution

4. **Data Population via CTAS**:
   - Primary method for large data volumes
   - Efficient single-operation approach
   - Alternative: INSERT INTO SELECT

5. **Python Automation**:
   - PAT authentication working
   - Can build repeatable orchestration
   - Supports error handling and logging

### ❌ Invalid Approaches:

1. **Bi-directional Metadata via Shares**:
   - Cannot write to shared database from consumer
   - Cannot use shares for coordination tables

2. **Simple String Replacement Only**:
   - Must validate for dynamic references
   - Must check session parameters
   - Must review stored procedure logic

3. **Relying Solely on OBJECT_DEPENDENCIES**:
   - Has limitations (see section 2)
   - Must be enhanced with additional discovery

### 🎯 Recommended Architecture:

**Phase 1**: Discovery (INFORMATION_SCHEMA + OBJECT_DEPENDENCIES + GET_DDL)
**Phase 2**: Share Creation (IMCUST → IMSDLC)
**Phase 3**: DDL Transformation (Automated + Manual Validation)
**Phase 4**: Object Creation + Data Population (CTAS)
**Phase 5**: Validation (Row counts, checksums, dependencies)
**Phase 6**: Grants (DataOps Team)
**Phase 7**: Cleanup + Documentation

---

## 8. References

All facts verified from official Snowflake documentation:

1. "About Secure Data Sharing" - Data shares are read-only
2. "OBJECT_DEPENDENCIES view" - Dependency tracking with limitations
3. "GET_DDL Function" - DDL extraction for all object types
4. "Snowflake Information Schema" - Metadata views (TABLES, VIEWS, PROCEDURES, etc.)
5. "CREATE TABLE AS SELECT" - CTAS for data population
6. "Authenticating with a programmatic access token (PAT)" - PAT authentication

**Documentation Source**: Snowflake MCP Documentation Server
**Verification Date**: 2025-11-09
**Status**: All key assumptions validated ✅

---

**Next Steps**:
1. Review MIGRATION_STRATEGY.md for detailed implementation plan
2. Implement Python orchestration scripts
3. Execute Phase 1 (Discovery) to get actual object inventory
4. Proceed with phased migration approach
