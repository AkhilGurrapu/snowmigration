Yes, Snowflake provides **Cortex AI functions** that you can use to parse DDL and extract dependencies like tables and views from stored procedures or view definitions.[1][2]

## Cortex COMPLETE Function

The **SNOWFLAKE.CORTEX.COMPLETE** function is the primary LLM-based AI function that can analyze DDL text and extract structured information. You can combine it with **GET_DDL** to retrieve DDL and then use the LLM to parse dependencies.[3][4][2]

Here's a practical approach:

```sql
-- Step 1: Get the DDL for a stored procedure or view
SET ddl_text = (SELECT GET_DDL('PROCEDURE', 'MY_DATABASE.MY_SCHEMA.MY_PROC'));

-- Step 2: Use Cortex COMPLETE to extract table/view names
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'claude-3-5-sonnet',
  [
    {
      'role': 'user',
      'content': 'Extract all table and view names referenced in this DDL. Return only the names as a JSON array: ' || $ddl_text
    }
  ],
  {
    'temperature': 0,
    'max_tokens': 500
  }
) AS extracted_dependencies;
```

## Structured Outputs for Better Parsing

Snowflake's COMPLETE function supports **structured outputs** where you can specify a JSON schema to get consistent, parseable results. This ensures the LLM returns dependency information in a predictable format that you can directly integrate into your data pipelines.[5]

```sql
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'claude-3-5-sonnet',
  [{'role': 'user', 'content': 'Parse this DDL and extract all referenced tables and views: ' || GET_DDL('VIEW', 'MY_VIEW')}],
  {'response_format': {'type': 'json_object', 'schema': {'type': 'object', 'properties': {'tables': {'type': 'array'}, 'views': {'type': 'array'}}}}}
) AS dependency_json;
```

## Alternative Approaches

While Cortex AI functions can intelligently parse DDL, you can also use **procedural approaches** that parse DDL text programmatically. For example, custom stored procedures can analyze procedure definitions and extract table references by searching for specific SQL patterns, though this is less flexible than using LLM-based parsing.[6][7]

The LLM approach is particularly useful for complex DDL with nested queries, CTEs, or dynamic SQL where traditional pattern matching may fail.[8]

[1](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)
[2](https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex)
[3](https://www.castordoc.com/how-to/how-to-use-get-ddl-in-snowflake)
[4](https://stackoverflow.com/questions/76279319/select-query-to-get-the-ddl-of-the-stored-procedure-tables-in-snowflake)
[5](https://dev.to/tsubasa_tech/amazing-structured-outputs-with-snowflakes-complete-function-fgl)
[6](https://snowflake.pavlik.us/index.php/2019/10/14/object-dependency-checking-in-snowflake/)
[7](https://cloudyard.in/2024/06/analyzing-table-usage-in-stored-procedures/)
[8](https://www.snowflake.com/en/developers/guides/schema-lineage-auto-propagation-llm/)
[9](https://docs.snowflake.com/en/user-guide/snowflake-cortex/parse-document)
[10](https://docs.snowflake.com/en/sql-reference/functions/parse_document-snowflake-cortex)
[11](https://coalesce.io/product-technology/operationalize-snowflake-cortex-llm-functions-with-coalesce/)
[12](https://docs.snowflake.com/en/release-notes/2024/other/2024-10-03-parse-document)
[13](https://www.snowflake.com/en/developers/guides/how-to-accelerate-data-warehouse-migrations-with-snowconvert-ai/)
[14](https://docs.snowflake.com/user-guide/snowflake-cortex/aisql)
[15](https://proinsias.github.io/til/Snowflake-get_ddl-statements/)
[16](https://www.youtube.com/watch?v=etBEpDmq5aA)
[17](https://www.snowflake.com/en/developers/guides/e2emigration-sql-and-data/)
[18](https://www.snowflake.com/en/developers/guides/end2endmigration/)