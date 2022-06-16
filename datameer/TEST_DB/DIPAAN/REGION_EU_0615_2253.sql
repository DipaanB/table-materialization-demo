-- stored procedure for the materialization of a Datameer transformation pipeline as a Snowflake table
-- there is no error handling and any exceptions are just raised
-- procedure does not return values; creates or replaces a Snowflake table as 'side-effect'
CREATE OR REPLACE PROCEDURE "REGION_EU_0615_2253"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT ='CREATED-BY-DATAMEER/ga11530merge/v1'
EXECUTE AS OWNER
AS
$$
DECLARE
  table_exists BOOLEAN;
BEGIN
  -- determine whether the table exists
  SELECT table_exists_field INTO :table_exists
  FROM (
    SELECT EXISTS (
      SELECT *
      FROM "INFORMATION_SCHEMA"."TABLES"
      WHERE
        table_schema = CURRENT_SCHEMA()
        AND table_name = 'REGION_EU_0615_2253'
    ) AS table_exists_field
  );

  -- materialize: create staging table
  CREATE OR REPLACE TABLE "REGION_EU_0615_2253_STAGING_20220616_025358" COMMENT="CREATED-BY-DATAMEER/ga11530merge/v1" AS
    -- TRANSFORMATION_SQL_START
    WITH
      "REGION" AS (SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."REGION")
    select * from REGION where R_NAME like 'EU%'
    -- TRANSFORMATION_SQL_END
    ;
  IF (table_exists) THEN
    -- swap existing table with staging table
    ALTER TABLE "REGION_EU_0615_2253"
    SWAP WITH "REGION_EU_0615_2253_STAGING_20220616_025358";
    -- drop staging table
    DROP TABLE "REGION_EU_0615_2253_STAGING_20220616_025358";
  ELSE
    -- rename staging table ...
    ALTER TABLE "REGION_EU_0615_2253_STAGING_20220616_025358"
    RENAME TO "REGION_EU_0615_2253";
  END IF;
END;
$$;
-- stored procedure for the materialization of a Datameer transformation pipeline as a Snowflake table including execution history tracking
-- includes the initial creation of an execution history table and records successful materializations as well as materialization errors
-- any exceptions during table materialization or execution history tracking are just raised
-- procedure does not return values; triggers a table materialization including execution history tracking as 'side-effect'
CREATE OR REPLACE PROCEDURE "DATAMEER_HISTORY_ga11530merge"(
  MATERIALIZATION_TRIGGER VARCHAR,
  USERNAME VARCHAR,
  TABLE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT ='CREATED-BY-DATAMEER/ga11530merge/v1'
AS
$$
BEGIN
  -- ensure that the materialization history table exists and add record before the actual materialization
  BEGIN
    -- create materialization history table 'DATAMEER_HISTORY_instance'
    CREATE TABLE IF NOT EXISTS "DATAMEER_HISTORY_ga11530merge"(
      NAME VARCHAR(16777216),
      TRIGGER_TYPE VARCHAR(16777216),
      USERNAME VARCHAR(16777216),
      WAREHOUSE VARCHAR(16777216),
      RECORD_COUNT NUMBER(38),
      BYTE_COUNT NUMBER(38),
      STATE VARCHAR(16777216),
      QUERY_START_TIME TIMESTAMP_LTZ(3),
      COMPLETED_TIME TIMESTAMP_LTZ(3),
      ERROR_MESSAGE VARCHAR(16777216)
    ) COMMENT = 'CREATED-BY-DATAMEER/ga11530merge/v1';

    -- insert materialization history entry
    INSERT INTO "DATAMEER_HISTORY_ga11530merge"(
      NAME,
      TRIGGER_TYPE,
      USERNAME,
      WAREHOUSE,
      STATE,
      QUERY_START_TIME)
    VALUES (
      :TABLE_NAME,
      :MATERIALIZATION_TRIGGER,
      :USERNAME,
      CURRENT_WAREHOUSE(),
      'EXECUTING',
      CURRENT_TIMESTAMP);
  END;

  -- perform materialization with error tracking
  BEGIN
    -- perform materialization
    EXECUTE IMMEDIATE 'CALL "' || TABLE_NAME || '"()';
  EXCEPTION
    WHEN OTHER THEN
      -- track materialization error
      LET error VARCHAR :=
        'SQLCODE=' || sqlcode || ',' ||
        'SQLSTATE=' || sqlstate || ',' ||
        'SQLERRM=' || sqlerrm;
      UPDATE
        "DATAMEER_HISTORY_ga11530merge"
      SET
        COMPLETED_TIME = CURRENT_TIMESTAMP,
        STATE ='FAILED',
        ERROR_MESSAGE = :error
      WHERE
        NAME = :TABLE_NAME
        AND COMPLETED_TIME IS NULL;
      RAISE;
  END;

  -- track materialization success
  DECLARE
    row_count NUMBER(38,0);
    byte_count NUMBER(38,0);
  BEGIN
    -- fetch record count and byte count
    SELECT
      ROW_COUNT,
      BYTES
    INTO
      :row_count,
      :byte_count
    FROM
      INFORMATION_SCHEMA.TABLES
    WHERE
      TABLE_CATALOG = CURRENT_DATABASE()
      AND TABLE_SCHEMA = CURRENT_SCHEMA()
      AND TABLE_NAME = :TABLE_NAME;

    -- track materialization success
    UPDATE
      "DATAMEER_HISTORY_ga11530merge"
    SET
      COMPLETED_TIME = TO_TIMESTAMP_LTZ(CURRENT_TIMESTAMP),
      STATE ='SUCCEEDED',
      RECORD_COUNT = :row_count,
      BYTE_COUNT = :byte_count
    WHERE
      NAME = :TABLE_NAME
      AND COMPLETED_TIME IS NULL;
  END;
END;
$$;
ALTER TASK IF EXISTS "REGION_EU_0615_2253" SUSPEND;
DROP TASK IF EXISTS "REGION_EU_0615_2253";
