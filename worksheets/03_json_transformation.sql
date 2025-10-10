/*************************************************************************************************************
WiFi Analytics Snowflake Demo - Data Transformation
1. VARIANT Data Type and JSON Processing
2. Dot-notation Extraction Techniques  
3. FLATTEN Operations for Nested Data
4. View Creation for Analytical Access
5. Data Type Casting and Performance
*************************************************************************************************************/

-- Ensure we're in the right context
USE ROLE NETWORK_ANALYST;
USE DATABASE WIFI_ANALYTICS;
USE WAREHOUSE WIFI_ANALYTICS_WH;

/*
    Data Transformation Demonstration
    This section showcases Snowflake's powerful capabilities for processing
    semi-structured JSON data using the VARIANT data type.
    
    Key learning objectives:
    - Understanding VARIANT storage and querying
    - Mastering dot-notation for JSON navigation
    - Using FLATTEN for complex nested structures
    - Creating analytical views from JSON data
*/

/*  1. VARIANT Data Type Exploration
    ****************************************************
    Explore the JSON telemetry data we ingested in the
    previous step. This demonstrates Snowflake's native
    JSON processing capabilities.
*/

-- Examine the raw JSON structure
SELECT 
    RECORD_ID,
    TELEMETRY_DATA,
    INGESTED_AT
FROM RAW.RAW_NETWORK_TELEMETRY
LIMIT 5;

-- Explore the nested JSON structure using dot notation
SELECT 
    RECORD_ID,
    TELEMETRY_DATA:network_telemetry:ap_id AS ap_id_raw,
    TELEMETRY_DATA:network_telemetry:timestamp AS timestamp_raw,
    TELEMETRY_DATA:network_telemetry:status_metrics:operational_status AS status_raw
FROM RAW.RAW_NETWORK_TELEMETRY
LIMIT 10;

/*  2. Dot-notation Extraction and Type Casting
    ****************************************************
    Extract specific values from JSON using colon operator (:)
    and demonstrate proper type casting for performance.
*/

-- Extract and cast JSON values to appropriate data types
SELECT 
    RECORD_ID,
    -- Extract and cast basic values
    TELEMETRY_DATA:network_telemetry:ap_id::INTEGER AS ap_id,
    TELEMETRY_DATA:network_telemetry:timestamp::TIMESTAMP_NTZ AS measurement_timestamp,
    
    -- Extract nested object values
    TELEMETRY_DATA:network_telemetry:status_metrics:operational_status::VARCHAR AS operational_status,
    TELEMETRY_DATA:network_telemetry:status_metrics:connected_clients::INTEGER AS connected_clients,
    
    -- Extract deeply nested values with casting
    TELEMETRY_DATA:network_telemetry:status_metrics:resource_utilization:cpu_percent::DECIMAL(5,2) AS cpu_utilization,
    TELEMETRY_DATA:network_telemetry:status_metrics:resource_utilization:memory_percent::DECIMAL(5,2) AS memory_utilization,
    
    -- Extract location context
    TELEMETRY_DATA:network_telemetry:location_context:building::VARCHAR AS building_name,
    TELEMETRY_DATA:network_telemetry:location_context:floor::INTEGER AS floor_number,
    TELEMETRY_DATA:network_telemetry:location_context:zone::VARCHAR AS zone_name
    
FROM RAW.RAW_NETWORK_TELEMETRY
ORDER BY ap_id, measurement_timestamp;

/*  3. FLATTEN Operations for Complex Structures
    ****************************************************
    While our current JSON is relatively simple, we'll demonstrate
    FLATTEN with a more complex scenario that might occur with
    device-level metrics arrays.
*/

-- Create a sample with array data to demonstrate FLATTEN
CREATE OR REPLACE TEMPORARY TABLE temp_complex_telemetry AS
SELECT PARSE_JSON(
    '{"network_telemetry": {' ||
        '"ap_id": 12345,' ||
        '"timestamp": "2024-01-15T10:30:00Z",' ||
        '"connected_devices": [' ||
            '{"mac": "AA:BB:CC:DD:EE:01", "signal_strength": -45, "data_usage_mb": 125.5},' ||
            '{"mac": "AA:BB:CC:DD:EE:02", "signal_strength": -62, "data_usage_mb": 89.2},' ||
            '{"mac": "AA:BB:CC:DD:EE:03", "signal_strength": -38, "data_usage_mb": 256.8}' ||
        ']' ||
    '}}'
) AS complex_telemetry_data;

-- Demonstrate FLATTEN to extract array elements
SELECT 
    complex_telemetry_data:network_telemetry:ap_id::INTEGER AS ap_id,
    complex_telemetry_data:network_telemetry:timestamp::TIMESTAMP_NTZ AS measurement_time,
    device.value:mac::VARCHAR AS device_mac,
    device.value:signal_strength::INTEGER AS signal_strength_dbm,
    device.value:data_usage_mb::DECIMAL(10,2) AS data_usage_mb
FROM temp_complex_telemetry,
LATERAL FLATTEN(INPUT => complex_telemetry_data:network_telemetry:connected_devices) device;

-- Clean up temporary table
DROP TABLE temp_complex_telemetry;

/*  4. Analytical View Creation
    ****************************************************
    Create a structured analytical view that transforms
    the JSON data into a queryable format for business users.
    This view will be used in governance and analytics sections.
*/

CREATE OR REPLACE VIEW ANALYTICS.VW_AP_PERFORMANCE AS
SELECT 
    -- Identifiers
    t.RECORD_ID,
    t.TELEMETRY_DATA:network_telemetry:ap_id::INTEGER AS ap_id,
    t.TELEMETRY_DATA:network_telemetry:timestamp::TIMESTAMP_NTZ AS measurement_timestamp,
    
    -- Join with dimension data for context
    ap.NETWORK_ID,
    n.NETWORK_NAME,
    n.CUSTOMER_NAME,
    n.INDUSTRY,
    ap.AP_MODEL,
    ap.MANUFACTURER,
    
    -- Performance metrics from JSON
    t.TELEMETRY_DATA:network_telemetry:status_metrics:operational_status::VARCHAR AS status,
    t.TELEMETRY_DATA:network_telemetry:status_metrics:connected_clients::INTEGER AS connected_clients,
    t.TELEMETRY_DATA:network_telemetry:status_metrics:resource_utilization:cpu_percent::DECIMAL(5,2) AS cpu_utilization_percent,
    t.TELEMETRY_DATA:network_telemetry:status_metrics:resource_utilization:memory_percent::DECIMAL(5,2) AS memory_utilization_percent,
    
    -- Location information (will be masked in governance section)
    t.TELEMETRY_DATA:network_telemetry:location_context:building::VARCHAR AS building_name,
    t.TELEMETRY_DATA:network_telemetry:location_context:floor::INTEGER AS floor_number,
    t.TELEMETRY_DATA:network_telemetry:location_context:zone::VARCHAR AS zone_name,
    
    -- Calculated fields for analysis
    CASE 
        WHEN connected_clients::FLOAT / ap.MAX_CLIENT_CAPACITY > 0.8 THEN 'High Load'
        WHEN connected_clients::FLOAT / ap.MAX_CLIENT_CAPACITY > 0.5 THEN 'Medium Load'
        ELSE 'Low Load'
    END AS load_category,
    
    -- Performance health score
    CASE 
        WHEN status = 'Offline' THEN 'Critical'
        WHEN cpu_utilization_percent > 80 OR memory_utilization_percent > 85 THEN 'Warning'
        WHEN connected_clients::FLOAT / ap.MAX_CLIENT_CAPACITY > 0.9 THEN 'Warning'
        ELSE 'Healthy'
    END AS health_status,
    
    t.INGESTED_AT AS data_ingested_timestamp

FROM RAW.RAW_NETWORK_TELEMETRY t
LEFT JOIN TRANSFORMED.DIM_ACCESS_POINTS ap 
    ON t.TELEMETRY_DATA:network_telemetry:ap_id::INTEGER = ap.AP_ID
LEFT JOIN TRANSFORMED.DIM_NETWORKS n 
    ON ap.NETWORK_ID = n.NETWORK_ID;

/*  5. Data Quality and Validation
    ****************************************************
    Verify the transformation results and data quality.
*/

-- Test the analytical view
SELECT 
    'JSON Records Processed' AS metric,
    COUNT(*) AS value
FROM ANALYTICS.VW_AP_PERFORMANCE
WHERE ap_id IS NOT NULL

UNION ALL

SELECT 
    'Unique Access Points in JSON',
    COUNT(DISTINCT ap_id)
FROM ANALYTICS.VW_AP_PERFORMANCE

UNION ALL

SELECT 
    'Records with Complete Data',
    COUNT(*)
FROM ANALYTICS.VW_AP_PERFORMANCE
WHERE ap_id IS NOT NULL 
    AND network_name IS NOT NULL 
    AND status IS NOT NULL;

-- Analyze data distribution by industry
SELECT 
    industry,
    COUNT(*) AS record_count,
    COUNT(DISTINCT ap_id) AS unique_aps,
    AVG(connected_clients) AS avg_client_load,
    AVG(cpu_utilization_percent) AS avg_cpu_utilization
FROM ANALYTICS.VW_AP_PERFORMANCE
WHERE industry IS NOT NULL
GROUP BY industry
ORDER BY record_count DESC;

-- Check health status distribution
SELECT 
    health_status,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM ANALYTICS.VW_AP_PERFORMANCE
GROUP BY health_status
ORDER BY record_count DESC;

/*  6. Performance Comparison: JSON vs Structured
    ****************************************************
    Compare query performance between JSON extraction
    and structured table queries.
*/

-- Query JSON data directly (slower)
SELECT 
    COUNT(*) AS total_records,
    AVG(TELEMETRY_DATA:network_telemetry:status_metrics:connected_clients::INTEGER) AS avg_clients
FROM RAW.RAW_NETWORK_TELEMETRY
WHERE TELEMETRY_DATA:network_telemetry:status_metrics:operational_status::VARCHAR = 'Online';

-- Query structured data (faster)
SELECT 
    COUNT(*) AS total_records,
    AVG(CONNECTED_CLIENT_COUNT) AS avg_clients
FROM TRANSFORMED.FACT_AP_STATUS
WHERE STATUS = 'Online';

/*
    Transformation demonstration complete!
    
    Key concepts demonstrated:
    1. VARIANT data type for flexible JSON storage
    2. Dot-notation (:) for navigating JSON structures  
    3. Type casting (::) for performance optimization
    4. FLATTEN for processing arrays within JSON
    5. View creation for analytical access
    6. Performance considerations for JSON vs structured queries
    
    Objects created:
    - VW_AP_PERFORMANCE: Analytical view combining JSON and dimensional data
    
    This view will be used in:
    - Part 1 Governance (dynamic masking demonstration)
    - Part 2 Analytics (semantic view foundation)
    
    Next step: Run 04_governance_rbac.sql
*/

-- Final verification that our view works correctly
SELECT 
    customer_name,
    manufacturer,
    COUNT(*) AS telemetry_records,
    COUNT(DISTINCT ap_id) AS unique_aps,
    SUM(CASE WHEN status = 'Online' THEN 1 ELSE 0 END) AS online_records,
    SUM(CASE WHEN health_status = 'Critical' THEN 1 ELSE 0 END) AS critical_records
FROM ANALYTICS.VW_AP_PERFORMANCE
WHERE customer_name IS NOT NULL
GROUP BY customer_name, manufacturer
ORDER BY telemetry_records DESC;
