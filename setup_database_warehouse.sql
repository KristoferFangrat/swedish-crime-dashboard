USE ROLE SYSADMIN;

-- Create Database
CREATE DATABASE IF NOT EXISTS crime_db;

-- Create Warehouse
CREATE WAREHOUSE IF NOT EXISTS crime_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- Create Roles
CREATE ROLE IF NOT EXISTS dlt_role;
CREATE ROLE IF NOT EXISTS dbt_role;
CREATE ROLE IF NOT EXISTS analyst_role;

-- Grant database privileges to DLT role
GRANT USAGE ON DATABASE crime_db TO ROLE dlt_role;
GRANT USAGE ON WAREHOUSE crime_wh TO ROLE dlt_role;
GRANT CREATE SCHEMA ON DATABASE crime_db TO ROLE dlt_role;

-- Grant schema and table privileges to DLT role (for staging)
GRANT ALL PRIVILEGES ON SCHEMA crime_db.PUBLIC TO ROLE dlt_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA crime_db.PUBLIC TO ROLE dlt_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA crime_db.PUBLIC TO ROLE dlt_role;

-- Grant database privileges to DBT role
GRANT USAGE ON DATABASE crime_db TO ROLE dbt_role;
GRANT USAGE ON WAREHOUSE crime_wh TO ROLE dbt_role;

-- DBT role can read from staging and write to analytics/mart
GRANT USAGE ON SCHEMA crime_db.PUBLIC TO ROLE dbt_role;
GRANT SELECT ON ALL TABLES IN SCHEMA crime_db.PUBLIC TO ROLE dbt_role;

-- Create MART schema for analytics outputs
CREATE SCHEMA IF NOT EXISTS crime_db.mart;

-- Grant schema privileges to DBT role for creating/modifying marts
GRANT ALL PRIVILEGES ON SCHEMA crime_db.mart TO ROLE dbt_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA crime_db.mart TO ROLE dbt_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA crime_db.mart TO ROLE dbt_role;

-- Grant analyst role read access to mart schema only
GRANT USAGE ON DATABASE crime_db TO ROLE analyst_role;
GRANT USAGE ON WAREHOUSE crime_wh TO ROLE analyst_role;
GRANT USAGE ON SCHEMA crime_db.mart TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA crime_db.mart TO ROLE analyst_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA crime_db.mart TO ROLE analyst_role;

-- Create staging table for police events
CREATE TABLE IF NOT EXISTS crime_db.PUBLIC.police_events_staging (
    event_id STRING,
    name STRING,
    description STRING,
    type STRING,
    location STRING,
    latitude FLOAT,
    longitude FLOAT,
    datetime STRING,
    affected_area STRING,
    api_response VARIANT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


USE ROLE ORGADMIN;
SHOW ACCOUNTS;