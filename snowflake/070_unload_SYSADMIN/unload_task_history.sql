
-- SYSADMIN
USE SCHEMA STITCH_DB.STAGE_1;

// Tasks history
CREATE TABLE IF NOT EXISTS
  TASKS (
    CREATED_ON TIMESTAMP_LTZ,
    NAME STRING,
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    OWNER STRING,
    COMMENT STRING,
    WAREHOUSE STRING,
    SCHEDULE STRING,
--    PREDECESSOR STRING,
    STATE STRING,
    DEFINITION STRING,
    CONDITION STRING,
    INGESTION_TIME TIMESTAMP_LTZ
);

// Task usage history
CREATE TABLE IF NOT EXISTS
  TASK_USAGE_HISTORY(
    QUERY_ID STRING,
    NAME STRING,
    DATABASE_NAME STRING,
    SCHEMA_NAME STRING,
    QUERY_TEXT STRING,
    CONDITION_TEXT STRING,
    STATE STRING,
    ERROR_CODE STRING,
    ERROR_MESSAGE STRING,
    SCHEDULED_TIME TIMESTAMP_LTZ,
    QUERY_START_TIME TIMESTAMP_LTZ,
    NEXT_SCHEDULED_TIME TIMESTAMP_LTZ,
    COMPLETED_TIME TIMESTAMP_LTZ,
    ROOT_TASK_ID STRING,
    GRAPH_VERSION NUMBER,
    RUN_ID NUMBER,
    RETURN_VALUE STRING,
    INGESTION_TIME TIMESTAMP_LTZ
  );
//===========================================================


//===========================================================
// tasks cdc and snapshotting with SYSADMIN
//===========================================================
// needed to monitor tasks as snowflake somehow does not have a MONITOR permission for tasks.
USE ROLE SYSADMIN;

CREATE OR REPLACE TASK TASK_HISTORY_TASK
  WAREHOUSE = STITCH_INGESTION_WH
  SCHEDULE = '5 minute'
AS
BEGIN
SHOW TASKS IN ACCOUNT;

INSERT INTO
  TASKS
SELECT
  "created_on" AS CREATED_ON,
  "name" AS NAME,
  "database_name" AS DATABASE_NAME,
  "schema_name" AS SCHEMA_NAME,
  "owner" AS OWNER,
  "comment" AS COMMENT,
  "warehouse" AS WAREHOUSE,
  "schedule" AS SCHEDULE,
--  "predecessor" AS PREDECESSOR,
  "state" AS STATE,
  "definition" AS DEFINITION,
  "condition" AS CONDITION,
  CURRENT_TIMESTAMP AS INGESTION_TIME
FROM
  TABLE(RESULT_SCAN(LAST_QUERY_ID()));

// Task usage history
SET CURSOR = (SELECT COALESCE(MAX(COMPLETED_TIME), 0::TIMESTAMP_LTZ) FROM TASK_USAGE_HISTORY);

INSERT INTO
  TASK_USAGE_HISTORY
SELECT
  QUERY_ID,
  NAME,
  DATABASE_NAME,
  SCHEMA_NAME,
  QUERY_TEXT,
  CONDITION_TEXT,
  STATE,
  ERROR_CODE,
  ERROR_MESSAGE,
  SCHEDULED_TIME,
  QUERY_START_TIME,
  NEXT_SCHEDULED_TIME,
  COMPLETED_TIME,
  ROOT_TASK_ID,
  GRAPH_VERSION,
  RUN_ID,
  RETURN_VALUE,
  CURRENT_TIMESTAMP AS INGESTION_TIME
FROM
  TABLE(INFORMATION_SCHEMA.TASK_HISTORY());
--WHERE
--  COMPLETED_TIME > $CURSOR;

COPY INTO @TASKS_STAGE from TASKS HEADER=TRUE OVERWRITE=TRUE;
COPY INTO @TASK_USAGE_HISTORY_STAGE from TASK_USAGE_HISTORY HEADER=TRUE OVERWRITE=TRUE;
END
//===========================================================

ALTER TASK TASK_HISTORY_TASK RESUME;
ALTER TASK TASK_HISTORY_TASK SUSPEND;