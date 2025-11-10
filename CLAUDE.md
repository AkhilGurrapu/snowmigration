## Enterprise Snowflake Cross-Account Migration**

### **Role & Context**
You are an expert Snowflake migration architect specializing in snowflake cross-account migrations within the same Snowflake organization. Your task is to design and implement a selective object migration strategy from account `imcust` (database: `prod_db`) to account `imsdlc` (database: `dev_db`), focusing on schemas `mart_investments_bolt` and `src_investments_bolt` with complete dependency resolution.


ASK/goal/prompt:
```
Great, so here we are having let's say the source account name is IMCUST and target account name is IMSDLC and we are migrating objects within source database which is prod_db into the target database which will be dev_db. the name of schema in 'mart_investments_bolt' schema same in both accounts/databases.
 I like the way you are just mentioned about using data shares and CTAS. So here for getting all the dependencies we are using SNOWFLAKE.CORE.GET_LINEAGE function within the snowflake where it lists all the tables/views which are upstream (as we are only focusing on upstream dependencies here) and once we got that dependencies we will extracting the DDLs for each and every object and then replacing the database name with the the target dev_db database name instead of prod_db database name and running the scripts on the target side and for this dependency objects only we are using data sharing where in between having a database role where we will be granting select on all these dependency objects to a database role and granting this database role to a data share so that it will be shared from source to target account(recommended process for data sharing) and then using CTAS in order to populate all these objects on target side. How does this plan sounds or does it complex? If it sounds good for you I want to automate this process where on the source side I will be just inputting the list of objects that I want to migrate with the database name, schema name, and object names and it should get all the dependencies and get all the DDLs and change the name of the database because the schema names in both source and target are the same so we are not worrying about the schema names here we are just worrying about the database names and giving me a list of DDL operations that needs to be executed on the target side and for all the upstream dependencies which will which needs to be added to a database role and then to an share so that it will be present on target side and with this collected DDLs will be run on target side and creating the CTAS. So I want all this to be automated and easier for any futuristic migration similar to this. I don't know what's the best approach is please do a deep search deep thing do Google search web search take time think thoroughly and give me the best possible solutions. If possible use stored procedures for this automation processes or anything that makes sense.
```

### **Authentication Setup**

**Configuration Files:**
- `config/connections.toml` - VS Code Snowflake extension configuration (both accounts)
- `config/imcust.yaml` - YAML format for IMCUST account
- `config/imsdlc.yaml` - YAML format for IMSDLC account

**Environment Variables (in `.env`):**
- `IMCUST_PAT` - Programmatic Access Token for imcust account
- `IMSDLC_PAT` - Programmatic Access Token for imsdlc account

**Connection Details:**
- Service Account: `svc4snowflakedeploy`
- Role: `ACCOUNTADMIN`
- Warehouse: `admin_wh`
- Both configurations use PAT (Programmatic Access Token) authentication

**VS Code Extension Setup:**
1. Open VS Code Snowflake extension
2. Click "Edit Connections File" 
3. Use the connections defined in `config/connections.toml`
4. Store PAT tokens in separate files as referenced in the config

**Python Connector:**
- Use the example in `config/python_connector_example.py`
- PAT tokens are loaded from environment variables for security

### **Migration Parameters**

**Source Environment:**
- Account: `imcust`
- Database: `prod_db`
- Schemas: `mart_investments_bolt`, 
- Status: Manually created (no IaC), fully operational production environment
- Remember both the schemas and database, along with objects alreday exists above, in imcust

**Target Environment:**
- Account: `imsdlc`
- Database: `dev_db`
- Schemas: Same naming convention as source
- Status: Schemas exist but objects need creation
- Requirement: Full object definitions + data migration
- both the schemas and database already exists in imsdlc