-- stored procedure for the materialization of a Datameer transformation pipeline as a Snowflake table
-- there is no error handling and any exceptions are just raised
-- procedure does not return values; creates or replaces a Snowflake table as 'side-effect'
CREATE OR REPLACE PROCEDURE "HOURLY_USAGE"()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT ='CREATED-BY-DATAMEER/dollyvarde57103/v1'
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
        AND table_name = 'HOURLY_USAGE'
    ) AS table_exists_field
  );

  -- materialize: create staging table
  CREATE OR REPLACE TABLE "HOURLY_USAGE_STAGING_20220616_212522" COMMENT="CREATED-BY-DATAMEER/dollyvarde57103/v1" AS
    -- TRANSFORMATION_SQL_START
    WITH
      "WAREHOUSE_METERING_HISTORY" AS (SELECT * FROM "STITCH_DB"."STAGE_2"."WAREHOUSE_METERING_HISTORY"),
      "HOURLY_USAGE_SQL" AS (with usage as (
      select * from WAREHOUSE_METERING_HISTORY
    ),
    
    usage_date_range as (
      select
        min(start_time)        as min_date,
        max(start_time)        as max_date
      from
        usage
    ),
    
    filler_hourly_usage as (
      select
        0 as credits_used,
        dateadd(
          hour, -seq4(), current_timestamp
        ) as start_time
      from
        table(generator(rowcount => 1000))
      where
        start_time >= (select min_date from usage_date_range) and
        start_time <= (select max_date from usage_date_range)
      order by 1
    ),
    
    combined_usage as (
      select usage.credits_used, usage.start_time from usage
      union all 
      select fdu.credits_used, fdu.start_time from filler_hourly_usage fdu
    ),
    
    hourly_usage as (
        select 
          to_number(sum(credits_used), 20, 2) as credits_used, 
          date_trunc('hour', start_time) as calculated_on 
        from 
          combined_usage 
        group by 
          calculated_on 
        order by 
          calculated_on desc
    )
    
    select * from hourly_usage)
    SELECT * FROM "HOURLY_USAGE_SQL"
    -- TRANSFORMATION_SQL_END
    ;
  IF (table_exists) THEN
    -- swap existing table with staging table
    ALTER TABLE "HOURLY_USAGE"
    SWAP WITH "HOURLY_USAGE_STAGING_20220616_212522";
    -- drop staging table
    DROP TABLE "HOURLY_USAGE_STAGING_20220616_212522";
  ELSE
    -- rename staging table ...
    ALTER TABLE "HOURLY_USAGE_STAGING_20220616_212522"
    RENAME TO "HOURLY_USAGE";
  END IF;
END;
$$;
-- stored procedure for the materialization of a Datameer transformation pipeline as a Snowflake table including execution history tracking
-- includes the initial creation of an execution history table and records successful materializations as well as materialization errors
-- any exceptions during table materialization or execution history tracking are just raised
-- procedure does not return values; triggers a table materialization including execution history tracking as 'side-effect'
CREATE OR REPLACE PROCEDURE "DATAMEER_HISTORY_dollyvarde57103"(
  MATERIALIZATION_TRIGGER VARCHAR,
  USERNAME VARCHAR,
  TABLE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT ='CREATED-BY-DATAMEER/dollyvarde57103/v1'
AS
$$
BEGIN
  -- ensure that the materialization history table exists and add record before the actual materialization
  BEGIN
    -- create materialization history table 'DATAMEER_HISTORY_instance'
    CREATE TABLE IF NOT EXISTS "DATAMEER_HISTORY_dollyvarde57103"(
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
    ) COMMENT = 'CREATED-BY-DATAMEER/dollyvarde57103/v1';

    -- insert materialization history entry
    INSERT INTO "DATAMEER_HISTORY_dollyvarde57103"(
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
        "DATAMEER_HISTORY_dollyvarde57103"
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
      "DATAMEER_HISTORY_dollyvarde57103"
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
ALTER TASK IF EXISTS "HOURLY_USAGE" SUSPEND;
DROP TASK IF EXISTS "HOURLY_USAGE";
