/*************************************************************************************************************
WiFi Analytics Snowflake Demo - Environment Setup
1. Database and Schema Creation
2. Role and Warehouse Configuration  
3. Permission Management
4. Context Setting
*************************************************************************************************************/


/*
    Environment Setup for WiFi Analytics Demo
    This script creates the foundational Snowflake objects needed for the demo.
    Run this first before proceeding to data generation.
*/

-- Use ACCOUNTADMIN for initial setup
USE ROLE ACCOUNTADMIN;

/*  1. Database and Schema Creation
    ****************************************************
    Create dedicated database and schemas for the demo.
    This provides isolation and clear organization.
*/

-- Create main database for WiFi analytics
CREATE DATABASE IF NOT EXISTS WIFI_ANALYTICS
    COMMENT = 'WiFi Network Analytics Demo Database';

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS WIFI_ANALYTICS.RAW
    COMMENT = 'Raw JSON telemetry data from WiFi networks';

CREATE SCHEMA IF NOT EXISTS WIFI_ANALYTICS.TRANSFORMED
    COMMENT = 'Structured tables transformed from raw JSON';

CREATE SCHEMA IF NOT EXISTS WIFI_ANALYTICS.ANALYTICS
    COMMENT = 'Analytical views and semantic models';

/*  2. Warehouse Creation
    ****************************************************
    Create dedicated warehouse for consistent performance
    during the demo.
*/

CREATE WAREHOUSE IF NOT EXISTS WIFI_ANALYTICS_WH
    WITH 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for WiFi Analytics Demo';

/*  3. Role Creation and Permissions
    ****************************************************
    Create role for network analyst persona with 
    appropriate permissions for the demo.
*/

-- Create role for network analysts
CREATE ROLE IF NOT EXISTS NETWORK_ANALYST
    COMMENT = 'Role for network administrators and analysts';

-- Grant database and schema permissions
GRANT USAGE ON DATABASE WIFI_ANALYTICS TO ROLE NETWORK_ANALYST;
GRANT USAGE ON SCHEMA WIFI_ANALYTICS.RAW TO ROLE NETWORK_ANALYST;
GRANT USAGE ON SCHEMA WIFI_ANALYTICS.TRANSFORMED TO ROLE NETWORK_ANALYST;
GRANT USAGE ON SCHEMA WIFI_ANALYTICS.ANALYTICS TO ROLE NETWORK_ANALYST;

-- Grant warehouse permissions
GRANT USAGE ON WAREHOUSE WIFI_ANALYTICS_WH TO ROLE NETWORK_ANALYST;

-- Grant table creation permissions for the demo
GRANT CREATE TABLE ON SCHEMA WIFI_ANALYTICS.RAW TO ROLE NETWORK_ANALYST;
GRANT CREATE TABLE ON SCHEMA WIFI_ANALYTICS.TRANSFORMED TO ROLE NETWORK_ANALYST;
GRANT CREATE VIEW ON SCHEMA WIFI_ANALYTICS.ANALYTICS TO ROLE NETWORK_ANALYST;
GRANT CREATE SEMANTIC VIEW ON SCHEMA WIFI_ANALYTICS.ANALYTICS TO ROLE NETWORK_ANALYST;
GRANT CREATE AGENT ON SCHEMA WIFI_ANALYTICS.ANALYTICS TO ROLE NETWORK_ANALYST;

-- Grant role to current user (adjust as needed)
GRANT ROLE NETWORK_ANALYST TO USER ["ADD USER HERE"];

/*  4. Set Context for Demo
    ****************************************************
    Configure session context for optimal demo experience.
*/

-- Switch to demo role and context
USE ROLE NETWORK_ANALYST;
USE DATABASE WIFI_ANALYTICS;
USE WAREHOUSE WIFI_ANALYTICS_WH;

-- Verify setup
SELECT 
    CURRENT_ROLE() AS current_role,
    CURRENT_DATABASE() AS current_database,
    CURRENT_WAREHOUSE() AS current_warehouse,
    CURRENT_TIMESTAMP() AS setup_timestamp;

-- List created objects for verification
SHOW DATABASES LIKE 'WIFI_ANALYTICS';
SHOW SCHEMAS IN DATABASE WIFI_ANALYTICS;
SHOW WAREHOUSES LIKE 'WIFI_ANALYTICS_WH';
SHOW ROLES LIKE 'NETWORK_ANALYST';

/*
    Environment setup complete!
    
    Next steps:
    1. Run 02_synthetic_data_creation.sql to generate sample data
    2. Proceed with Part 1 transformation exercises
    
    Objects created:
    - Database: WIFI_ANALYTICS
    - Schemas: RAW, TRANSFORMED, ANALYTICS  
    - Warehouse: WIFI_ANALYTICS_WH
    - Role: NETWORK_ANALYST
*/
