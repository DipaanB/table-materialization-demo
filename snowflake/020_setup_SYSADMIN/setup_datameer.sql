// Create top level objects
CREATE DATABASE IF NOT EXISTS
    DATAMEER_DB
    COMMENT='Database for Datameer transformations';

CREATE SCHEMA IF NOT EXISTS
    DATAMEER_DB.SNOWFLAKE_USAGE
    COMMENT='Schema for Datameer SNOWFLAKE_USAGE transformations';

CREATE WAREHOUSE IF NOT EXISTS
        DATAMEER_TRANSFORM_WH
        COMMENT='Warehouse for Datameer transformations'
        WAREHOUSE_SIZE=XSMALL
        AUTO_SUSPEND=60
        INITIALLY_SUSPENDED=TRUE;

// Transfer ownerships
GRANT OWNERSHIP ON DATABASE DATAMEER_DB                    TO ROLE DATAMEER_SERVICE_ROLE;
GRANT OWNERSHIP ON SCHEMA DATAMEER_DB.SNOWFLAKE_USAGE      TO ROLE DATAMEER_SERVICE_ROLE;
GRANT OWNERSHIP ON WAREHOUSE DATAMEER_TRANSFORM_WH         TO ROLE DATAMEER_SERVICE_ROLE;

// grant warehouse usage to admins
GRANT USAGE ON WAREHOUSE DATAMEER_TRANSFORM_WH             TO ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE DATAMEER_TRANSFORM_WH             TO ROLE SYSADMIN;
