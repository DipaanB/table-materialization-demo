// Set Datameer service account defaults and permissions
ALTER USER
  DATAMEER_SERVICE_ACCOUNT
SET
  DEFAULT_ROLE=DATAMEER_SERVICE_ROLE
  DEFAULT_WAREHOUSE=DATAMEER_TRANSFORM_WH;

GRANT USAGE ON DATABASE DATAMEER_DB                                 TO ROLE DATAMEER_SERVICE_ROLE;
GRANT USAGE ON SCHEMA DATAMEER_DB.SNOWFLAKE_USAGE                   TO ROLE DATAMEER_SERVICE_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DATAMEER_DB.SNOWFLAKE_USAGE    TO ROLE DATAMEER_SERVICE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DATAMEER_DB.SNOWFLAKE_USAGE TO ROLE DATAMEER_SERVICE_ROLE;

GRANT CREATE TABLE ON SCHEMA DATAMEER_DB.SNOWFLAKE_USAGE            TO ROLE DATAMEER_SERVICE_ROLE;
GRANT CREATE STAGE ON SCHEMA DATAMEER_DB.SNOWFLAKE_USAGE            TO ROLE DATAMEER_SERVICE_ROLE;
GRANT CREATE FILE FORMAT ON SCHEMA DATAMEER_DB.SNOWFLAKE_USAGE      TO ROLE DATAMEER_SERVICE_ROLE;
GRANT CREATE TASK ON SCHEMA DATAMEER_DB.SNOWFLAKE_USAGE             TO ROLE DATAMEER_SERVICE_ROLE;

GRANT USAGE, OPERATE ON WAREHOUSE DATAMEER_TRANSFORM_WH             TO ROLE DATAMEER_SERVICE_ROLE;
