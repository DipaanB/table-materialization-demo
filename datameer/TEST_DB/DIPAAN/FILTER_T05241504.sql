-- script for the materialization of a Datameer transformation pipeline as a Snowflake table
-- there is no error handling and any exceptions are just raised
EXECUTE IMMEDIATE
$$
BEGIN
  -- materialize
  CREATE OR REPLACE TABLE "TEST_DB"."DIPAAN"."FILTER_T05241504" (
    "R_REGIONKEY" NUMBER(38),
    "R_NAME" VARCHAR(25),
    "R_COMMENT" VARCHAR(152)
  ) COMMENT="CREATED-BY-DATAMEER/elt-14563-9577ad/v5" AS (
    SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."REGION" WHERE ("R_NAME" like '%EU%')
  );

  -- return metadata
  LET rs RESULTSET := (WITH
    query_history AS (
      SELECT *
      FROM TABLE("TEST_DB".INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION()) qh
      WHERE qh.query_id = LAST_QUERY_ID()
    ),
    table_info AS (
      SELECT *
      FROM "TEST_DB".INFORMATION_SCHEMA.TABLES t
      WHERE t.table_catalog = 'TEST_DB'
      AND t.table_schema = 'DIPAAN'
      AND t.table_name = 'FILTER_T05241504'
    )
    SELECT *
    FROM table_info JOIN query_history);
  RETURN TABLE(rs);
END;
$$
;
