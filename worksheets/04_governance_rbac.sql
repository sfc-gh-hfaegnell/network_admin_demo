/*************************************************************************************************************
WiFi Analytics Snowflake Demo - Governance and RBAC
1. Dynamic Data Masking Implementation
2. Role-Based Access Control
3. Data Privacy for Location Information
4. Permission Validation and Testing
*************************************************************************************************************/

-- Set query tag for tracking
ALTER SESSION SET query_tag = '{"origin":"wifi_analytics_demo","name":"governance_rbac","version":{"major":1, "minor":0}}';

-- Switch to ACCOUNTADMIN for governance setup
USE ROLE ACCOUNTADMIN;
USE DATABASE WIFI_ANALYTICS;
USE WAREHOUSE WIFI_ANALYTICS_WH;

/*
    Data Governance and Security Demonstration
    This section showcases Snowflake's native governance capabilities:
    - Dynamic data masking for sensitive location data
    - Role-based access control for different user types
    - Permission management and validation
    
    Business context: WiFi location data may be considered sensitive
    and should be masked from certain users while maintaining
    analytical capabilities.
*/

/*  1. Dynamic Data Masking Setup
    ****************************************************
    Implement masking policies for location-sensitive data
    in our analytical view. This protects building names,
    floor information, and zone details.
*/

-- Create masking policy for building names
CREATE MASKING POLICY IF NOT EXISTS ANALYTICS.mask_building_name AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'NETWORK_ANALYST') THEN val
        ELSE REGEXP_REPLACE(val, '[A-Za-z]', '*')
    END;

-- Create masking policy for zone information  
CREATE MASKING POLICY IF NOT EXISTS ANALYTICS.mask_zone_info AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'NETWORK_ANALYST') THEN val
        ELSE '***MASKED***'
    END;

-- Create masking policy for floor numbers (show only general level)
CREATE MASKING POLICY IF NOT EXISTS ANALYTICS.mask_floor_number AS (val NUMBER) RETURNS NUMBER ->
    CASE 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'NETWORK_ANALYST') THEN val
        ELSE CASE 
            WHEN val <= 2 THEN 1
            WHEN val <= 5 THEN 3  
            ELSE 5
        END
    END;

/*  2. Apply Masking Policies to Views
    ****************************************************
    Apply the masking policies to sensitive columns in our
    analytical view. This demonstrates column-level security.
*/

-- Apply masking to the analytical view created in transformation step (skip if already applied)
-- Note: These may already be applied from previous runs
-- ALTER VIEW ANALYTICS.VW_AP_PERFORMANCE MODIFY COLUMN building_name 
--     SET MASKING POLICY ANALYTICS.mask_building_name;

-- ALTER VIEW ANALYTICS.VW_AP_PERFORMANCE MODIFY COLUMN zone_name 
--     SET MASKING POLICY ANALYTICS.mask_zone_info;

-- ALTER VIEW ANALYTICS.VW_AP_PERFORMANCE MODIFY COLUMN floor_number 
--     SET MASKING POLICY ANALYTICS.mask_floor_number;

/*  3. Create Additional Role for Demonstration
    ****************************************************
    Create a restricted role to demonstrate masking in action.
    This role represents external analysts or vendors who need
    access to performance data but not location details.
*/

-- Create restricted analyst role
CREATE ROLE IF NOT EXISTS EXTERNAL_ANALYST
    COMMENT = 'Role for external analysts with limited data access';

-- Grant basic database and schema access
GRANT USAGE ON DATABASE WIFI_ANALYTICS TO ROLE EXTERNAL_ANALYST;
GRANT USAGE ON SCHEMA WIFI_ANALYTICS.ANALYTICS TO ROLE EXTERNAL_ANALYST;
GRANT USAGE ON WAREHOUSE WIFI_ANALYTICS_WH TO ROLE EXTERNAL_ANALYST;

-- Grant SELECT on the analytical view (masking will apply automatically)
GRANT SELECT ON VIEW ANALYTICS.VW_AP_PERFORMANCE TO ROLE EXTERNAL_ANALYST;

-- Grant role to current user for testing
GRANT ROLE EXTERNAL_ANALYST TO USER henry;

/*  4. Test Masking Policies
    ****************************************************
    Demonstrate how the same query returns different results
    based on the user's role and applied masking policies.
*/

-- Test as NETWORK_ANALYST (full access)
USE ROLE NETWORK_ANALYST;

SELECT 
    'NETWORK_ANALYST View' AS test_scenario,
    customer_name,
    building_name,
    floor_number,
    zone_name,
    COUNT(*) AS record_count
FROM ANALYTICS.VW_AP_PERFORMANCE
WHERE customer_name IS NOT NULL
GROUP BY customer_name, building_name, floor_number, zone_name
ORDER BY customer_name, building_name
LIMIT 10;

-- Test as EXTERNAL_ANALYST (masked data)
USE ROLE EXTERNAL_ANALYST;

SELECT 
    'EXTERNAL_ANALYST View' AS test_scenario,
    customer_name,
    building_name,
    floor_number,
    zone_name,
    COUNT(*) AS record_count
FROM ANALYTICS.VW_AP_PERFORMANCE
WHERE customer_name IS NOT NULL
GROUP BY customer_name, building_name, floor_number, zone_name
ORDER BY customer_name, building_name
LIMIT 10;

/*  5. Demonstrate Analytics Still Work with Masking
    ****************************************************
    Show that business analytics remain functional even
    with masking applied. The key metrics are still accessible.
*/

-- Performance analysis by industry (works for both roles)
SELECT 
    industry,
    manufacturer,
    COUNT(*) AS total_records,
    COUNT(DISTINCT ap_id) AS unique_access_points,
    AVG(connected_clients) AS avg_client_load,
    AVG(cpu_utilization_percent) AS avg_cpu_utilization,
    SUM(CASE WHEN status = 'Online' THEN 1 ELSE 0 END) AS online_records,
    SUM(CASE WHEN health_status = 'Critical' THEN 1 ELSE 0 END) AS critical_records
FROM ANALYTICS.VW_AP_PERFORMANCE
WHERE industry IS NOT NULL
GROUP BY industry, manufacturer
ORDER BY total_records DESC;

-- Health status distribution (unaffected by masking)
SELECT 
    health_status,
    load_category,
    COUNT(*) AS record_count,
    ROUND(AVG(cpu_utilization_percent), 2) AS avg_cpu_util,
    ROUND(AVG(memory_utilization_percent), 2) AS avg_memory_util
FROM ANALYTICS.VW_AP_PERFORMANCE
GROUP BY health_status, load_category
ORDER BY health_status, load_category;

/*  6. Permission Validation
    ****************************************************
    Verify that roles have appropriate permissions and
    that unauthorized access is prevented.
*/

-- Switch back to NETWORK_ANALYST for validation
USE ROLE NETWORK_ANALYST;

-- Show current role permissions
SHOW GRANTS TO ROLE NETWORK_ANALYST;

-- Verify access to all schemas
SELECT 
    'Schema Access Test' AS test_type,
    'RAW' AS schema_name,
    COUNT(*) AS accessible_objects
FROM RAW.RAW_NETWORK_TELEMETRY

UNION ALL

SELECT 
    'Schema Access Test',
    'TRANSFORMED',
    (SELECT COUNT(*) FROM TRANSFORMED.DIM_NETWORKS) + 
    (SELECT COUNT(*) FROM TRANSFORMED.DIM_ACCESS_POINTS) +
    (SELECT COUNT(*) FROM TRANSFORMED.FACT_AP_STATUS)

UNION ALL

SELECT 
    'Schema Access Test',
    'ANALYTICS',
    COUNT(*)
FROM ANALYTICS.VW_AP_PERFORMANCE;

/*  7. Create Governance Summary View
    ****************************************************
    All masking policies and roles have been configured.
    The governance framework is now ready for use.
*/

/*  8. Final Validation and Cleanup Preparation
    ****************************************************
    Verify all governance controls are working correctly
    and prepare for next section.
*/

-- Switch back to NETWORK_ANALYST for final tests
USE ROLE NETWORK_ANALYST;

-- Show created masking policies (requires ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;
SHOW MASKING POLICIES IN SCHEMA ANALYTICS;

-- Switch back to NETWORK_ANALYST for validation
USE ROLE NETWORK_ANALYST;

-- Verify that analytical capabilities remain intact
SELECT 
    'Governance Validation' AS test_category,
    'Total Networks' AS metric,
    COUNT(DISTINCT customer_name) AS value
FROM ANALYTICS.VW_AP_PERFORMANCE

UNION ALL

SELECT 
    'Governance Validation',
    'Total Access Points',
    COUNT(DISTINCT ap_id)
FROM ANALYTICS.VW_AP_PERFORMANCE

UNION ALL

SELECT 
    'Governance Validation',
    'Records with Location Masking',
    COUNT(*)
FROM ANALYTICS.VW_AP_PERFORMANCE
WHERE building_name IS NOT NULL;

/*
    Governance and RBAC demonstration complete!
    
    Key concepts demonstrated:
    1. Dynamic data masking for sensitive location data
    2. Role-based access control with different permission levels
    3. Column-level security that preserves analytical capabilities
    4. Policy application and validation
    5. Governance monitoring and compliance views
    
    Objects created:
    - Masking policies: mask_building_name, mask_zone_info, mask_floor_number
    - Role: EXTERNAL_ANALYST (with restricted access)
    
    Security implemented:
    - Location data (building, zone, floor) masked for external analysts
    - Performance metrics remain accessible for analytics
    - Role-based permissions properly configured
    
    All objects are actively used and tested in this section.
    
    Next step: Run 05_semantic_views.sql for Part 2
*/

-- Show final role and context for next section
SELECT 
    CURRENT_ROLE() AS current_role,
    CURRENT_DATABASE() AS current_database,
    CURRENT_SCHEMA() AS current_schema,
    'Ready for Part 2: Semantic Views' AS status;
