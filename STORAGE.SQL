SELECT
  SUM(size_kb) AS total_lineage_size_kb
FROM (
    SELECT
      l.db,
      l.schema,
      l.name,
      ROUND(t.bytes / 1024, 2) AS size_kb
    FROM (
        SELECT
          TARGET_OBJECT_DATABASE AS db,
          TARGET_OBJECT_SCHEMA AS schema,
          TARGET_OBJECT_NAME AS name,
          TARGET_OBJECT_DOMAIN AS domain
        FROM
          TABLE(SNOWFLAKE.CORE.GET_LINEAGE(
            'PROD_DB.MART_HR_BOLT.FACT_TIME_ENTRIES',
            'TABLE',
            'UPSTREAM',
            5
            )
        )
        WHERE
          TARGET_OBJECT_DOMAIN = 'TABLE'
    ) l
    JOIN prod_db.information_schema.tables t
      ON l.schema = t.table_schema
      AND l.name = t.table_name
);