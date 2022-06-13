-- ACCOUNTADMIN

//===========================================================
// initial schema setup
//===========================================================
// schema
CREATE SCHEMA IF NOT EXISTS STITCH_DB.STAGE_1;
USE SCHEMA STITCH_DB.STAGE_1;

// warehouse metering history
CREATE TABLE IF NOT EXISTS
  WAREHOUSE_METERING_HISTORY
AS (
  SELECT
    START_TIME,
    END_TIME,
    WAREHOUSE_ID,
    WAREHOUSE_NAME,
    CREDITS_USED,
    CREDITS_USED_COMPUTE,
    CREDITS_USED_CLOUD_SERVICES,
    CURRENT_TIMESTAMP AS INGESTION_TIME
  FROM
    SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
);

// query history
CREATE TABLE IF NOT EXISTS
  QUERY_HISTORY
AS (
  SELECT
    QUERY_ID,
    QUERY_TEXT,
    DATABASE_ID,
    DATABASE_NAME,
    SCHEMA_ID,
    SCHEMA_NAME,
    QUERY_TYPE,
    SESSION_ID,
    USER_NAME,
    ROLE_NAME,
    WAREHOUSE_ID,
    WAREHOUSE_NAME,
    WAREHOUSE_SIZE,
    WAREHOUSE_TYPE,
    CLUSTER_NUMBER,
    QUERY_TAG,
    EXECUTION_STATUS,
    ERROR_CODE,
    ERROR_MESSAGE,
    START_TIME,
    END_TIME,
    TOTAL_ELAPSED_TIME,
    BYTES_SCANNED,
    PERCENTAGE_SCANNED_FROM_CACHE,
    BYTES_WRITTEN,
    BYTES_WRITTEN_TO_RESULT,
    BYTES_READ_FROM_RESULT,
    ROWS_PRODUCED,
    ROWS_INSERTED,
    ROWS_UPDATED,
    ROWS_DELETED,
    ROWS_UNLOADED,
    BYTES_DELETED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    BYTES_SPILLED_TO_LOCAL_STORAGE,
    BYTES_SPILLED_TO_REMOTE_STORAGE,
    BYTES_SENT_OVER_THE_NETWORK,
    COMPILATION_TIME,
    EXECUTION_TIME,
    QUEUED_PROVISIONING_TIME,
    QUEUED_REPAIR_TIME,
    QUEUED_OVERLOAD_TIME,
    TRANSACTION_BLOCKED_TIME,
    OUTBOUND_DATA_TRANSFER_CLOUD,
    OUTBOUND_DATA_TRANSFER_REGION,
    OUTBOUND_DATA_TRANSFER_BYTES,
    INBOUND_DATA_TRANSFER_CLOUD,
    INBOUND_DATA_TRANSFER_REGION,
    INBOUND_DATA_TRANSFER_BYTES,
    LIST_EXTERNAL_FILES_TIME,
    CREDITS_USED_CLOUD_SERVICES,
    CURRENT_TIMESTAMP AS INGESTION_TIME
  FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
);

//===========================================================
// account_usage cdc and snapshotting
//===========================================================

CREATE OR REPLACE TASK WAREHOUSE_METERING_HISTORY_TASK
  WAREHOUSE = STITCH_INGESTION_WH
  SCHEDULE = '5 minute'
AS
BEGIN
SET CURSOR = (SELECT COALESCE(MAX(START_TIME), 0::TIMESTAMP_LTZ) FROM WAREHOUSE_METERING_HISTORY);

INSERT INTO
  WAREHOUSE_METERING_HISTORY
SELECT
  START_TIME,
  END_TIME,
  WAREHOUSE_ID,
  WAREHOUSE_NAME,
  CREDITS_USED,
  CREDITS_USED_COMPUTE,
  CREDITS_USED_CLOUD_SERVICES,
  CURRENT_TIMESTAMP AS INGESTION_TIME
FROM
  SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;
--WHERE
--  START_TIME > $CURSOR;

COPY INTO @WAREHOUSE_METERING_HISTORY_STAGE from WAREHOUSE_METERING_HISTORY HEADER = TRUE OVERWRITE=TRUE;
END

//===========================================================

CREATE OR REPLACE TASK QUERY_HISTORY_TASK
  WAREHOUSE = STITCH_INGESTION_WH
  SCHEDULE = '5 minute'
AS
BEGIN
SET CURSOR = (SELECT COALESCE(MAX(END_TIME), 0::TIMESTAMP_LTZ) FROM QUERY_HISTORY);

INSERT INTO
  QUERY_HISTORY
SELECT
  QUERY_ID,
  QUERY_TEXT,
  DATABASE_ID,
  DATABASE_NAME,
  SCHEMA_ID,
  SCHEMA_NAME,
  QUERY_TYPE,
  SESSION_ID,
  USER_NAME,
  ROLE_NAME,
  WAREHOUSE_ID,
  WAREHOUSE_NAME,
  WAREHOUSE_SIZE,
  WAREHOUSE_TYPE,
  CLUSTER_NUMBER,
  QUERY_TAG,
  EXECUTION_STATUS,
  ERROR_CODE,
  ERROR_MESSAGE,
  START_TIME,
  END_TIME,
  TOTAL_ELAPSED_TIME,
  BYTES_SCANNED,
  PERCENTAGE_SCANNED_FROM_CACHE,
  BYTES_WRITTEN,
  BYTES_WRITTEN_TO_RESULT,
  BYTES_READ_FROM_RESULT,
  ROWS_PRODUCED,
  ROWS_INSERTED,
  ROWS_UPDATED,
  ROWS_DELETED,
  ROWS_UNLOADED,
  BYTES_DELETED,
  PARTITIONS_SCANNED,
  PARTITIONS_TOTAL,
  BYTES_SPILLED_TO_LOCAL_STORAGE,
  BYTES_SPILLED_TO_REMOTE_STORAGE,
  BYTES_SENT_OVER_THE_NETWORK,
  COMPILATION_TIME,
  EXECUTION_TIME,
  QUEUED_PROVISIONING_TIME,
  QUEUED_REPAIR_TIME,
  QUEUED_OVERLOAD_TIME,
  TRANSACTION_BLOCKED_TIME,
  OUTBOUND_DATA_TRANSFER_CLOUD,
  OUTBOUND_DATA_TRANSFER_REGION,
  OUTBOUND_DATA_TRANSFER_BYTES,
  INBOUND_DATA_TRANSFER_CLOUD,
  INBOUND_DATA_TRANSFER_REGION,
  INBOUND_DATA_TRANSFER_BYTES,
  LIST_EXTERNAL_FILES_TIME,
  CREDITS_USED_CLOUD_SERVICES,
  CURRENT_TIMESTAMP AS INGESTION_TIME
FROM
  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY;
--WHERE
--  END_TIME > $CURSOR;

COPY INTO @QUERY_HISTORY_STAGE from QUERY_HISTORY HEADER = TRUE OVERWRITE=TRUE;
END

//===========================================================

ALTER TASK WAREHOUSE_METERING_HISTORY_TASK RESUME;
ALTER TASK QUERY_HISTORY_TASK RESUME;

ALTER TASK QUERY_HISTORY_TASK SUSPEND;
