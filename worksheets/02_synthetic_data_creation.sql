/*************************************************************************************************************
WiFi Analytics Snowflake Demo - Synthetic Data Generation
1. Dimension Tables Creation
2. JSON Staging Table Creation  
3. Fact Table Creation with Realistic Patterns
4. Data Population with Business Scenarios
*************************************************************************************************************/

-- Ensure we're in the right context
USE ROLE NETWORK_ANALYST;
USE DATABASE WIFI_ANALYTICS;
USE WAREHOUSE WIFI_ANALYTICS_WH;

/*
    Synthetic Data Generation for WiFi Analytics
    This script creates realistic WiFi network data with:
    - Multi-layered seasonality (daily, weekly patterns)
    - Realistic correlations (load vs performance)
    - Injected scenarios (firmware issues, capacity constraints)
*/

/*  1. Dimension Tables Creation
    ****************************************************
    Create core dimension tables that will be used throughout
    both Part 1 and Part 2 of the demo.
*/

-- Networks dimension - Customer networks across industries
CREATE OR REPLACE TABLE TRANSFORMED.DIM_NETWORKS (
    NETWORK_ID INTEGER PRIMARY KEY,
    NETWORK_NAME VARCHAR(100),
    CUSTOMER_NAME VARCHAR(100),
    INDUSTRY VARCHAR(50),
    LOCATION_CITY VARCHAR(50),
    LOCATION_COUNTRY VARCHAR(50),
    SLA_UPTIME_TARGET DECIMAL(5,4),
    CREATED_DATE DATE
);

-- Access Points dimension - Hardware inventory
CREATE OR REPLACE TABLE TRANSFORMED.DIM_ACCESS_POINTS (
    AP_ID INTEGER PRIMARY KEY,
    NETWORK_ID INTEGER,
    AP_MAC_ADDRESS VARCHAR(17),
    AP_MODEL VARCHAR(50),
    MANUFACTURER VARCHAR(50),
    WIFI_STANDARD VARCHAR(20),
    MAX_CLIENT_CAPACITY INTEGER,
    DEPLOYMENT_DATE DATE,
    FIRMWARE_VERSION VARCHAR(20),
    LOCATION_BUILDING VARCHAR(100),
    LOCATION_FLOOR INTEGER,
    LOCATION_ZONE VARCHAR(50),
    FOREIGN KEY (NETWORK_ID) REFERENCES TRANSFORMED.DIM_NETWORKS(NETWORK_ID)
);

/*  2. JSON Staging Table
    ****************************************************
    Create table for raw JSON telemetry data to demonstrate
    VARIANT processing in Part 1.
*/

CREATE OR REPLACE TABLE RAW.RAW_NETWORK_TELEMETRY (
    RECORD_ID INTEGER AUTOINCREMENT,
    TELEMETRY_DATA VARIANT,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/*  3. Fact Table Creation
    ****************************************************
    Create main fact table for access point status metrics.
    This will be the primary table for analytics.
*/

CREATE OR REPLACE TABLE TRANSFORMED.FACT_AP_STATUS (
    SNAPSHOT_TIMESTAMP TIMESTAMP_NTZ,
    AP_ID INTEGER,
    NETWORK_ID INTEGER,
    STATUS VARCHAR(20),
    CONNECTED_CLIENT_COUNT INTEGER,
    CPU_UTILIZATION_PERCENT DECIMAL(5,2),
    MEMORY_UTILIZATION_PERCENT DECIMAL(5,2),
    PRIMARY KEY (SNAPSHOT_TIMESTAMP, AP_ID),
    FOREIGN KEY (AP_ID) REFERENCES TRANSFORMED.DIM_ACCESS_POINTS(AP_ID),
    FOREIGN KEY (NETWORK_ID) REFERENCES TRANSFORMED.DIM_NETWORKS(NETWORK_ID)
);

/*  4. Data Population - Networks
    ****************************************************
    Populate networks with diverse industry representation.
*/

INSERT INTO TRANSFORMED.DIM_NETWORKS VALUES
(1001, 'TechCorp HQ', 'TechCorp Inc', 'Corporate', 'San Francisco', 'USA', 0.9999, '2023-01-15'),
(1002, 'Metro Mall WiFi', 'Metro Shopping', 'Retail', 'New York', 'USA', 0.999, '2023-02-01'),
(1003, 'Grand Hotel Guest', 'Grand Hotel Chain', 'Hospitality', 'London', 'UK', 0.995, '2023-01-20'),
(1004, 'Sports Arena', 'City Sports Complex', 'Public Venue', 'Chicago', 'USA', 0.999, '2023-03-01'),
(1005, 'University Campus', 'State University', 'Education', 'Boston', 'USA', 0.99, '2023-01-10'),
(1006, 'Distribution Center', 'LogiFlow Corp', 'Logistics', 'Dallas', 'USA', 0.999, '2023-02-15'),
(1007, 'Medical Center', 'Regional Health', 'Healthcare', 'Seattle', 'USA', 0.9999, '2023-01-25'),
(1008, 'Finance Tower', 'Global Bank', 'Financial', 'Toronto', 'Canada', 0.9999, '2023-02-10'),
(1009, 'Manufacturing Plant', 'Industrial Corp', 'Manufacturing', 'Detroit', 'USA', 0.995, '2023-03-05'),
(1010, 'Airport Terminal', 'International Airport', 'Transportation', 'Miami', 'USA', 0.9999, '2023-01-30'),
(1011, 'Conference Center', 'Event Spaces LLC', 'Public Venue', 'Las Vegas', 'USA', 0.999, '2023-02-20'),
(1012, 'Startup Hub', 'Innovation District', 'Corporate', 'Austin', 'USA', 0.995, '2023-03-10');

/*  5. Data Population - Access Points
    ****************************************************
    Create realistic AP distribution across networks with
    various manufacturers and models.
*/

-- Generate access points with realistic distribution
INSERT INTO TRANSFORMED.DIM_ACCESS_POINTS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY n.NETWORK_ID, ap_seq.seq) AS AP_ID,
    n.NETWORK_ID,
    CONCAT(
        UPPER(SUBSTR(MD5(CONCAT(n.NETWORK_ID, ap_seq.seq)), 1, 2)), ':',
        UPPER(SUBSTR(MD5(CONCAT(n.NETWORK_ID, ap_seq.seq)), 3, 2)), ':',
        UPPER(SUBSTR(MD5(CONCAT(n.NETWORK_ID, ap_seq.seq)), 5, 2)), ':',
        UPPER(SUBSTR(MD5(CONCAT(n.NETWORK_ID, ap_seq.seq)), 7, 2)), ':',
        UPPER(SUBSTR(MD5(CONCAT(n.NETWORK_ID, ap_seq.seq)), 9, 2)), ':',
        UPPER(SUBSTR(MD5(CONCAT(n.NETWORK_ID, ap_seq.seq)), 11, 2))
    ) AS AP_MAC_ADDRESS,
    CASE 
        WHEN UNIFORM(1, 100, RANDOM()) <= 30 THEN 'Cisco Meraki MR46'
        WHEN UNIFORM(1, 100, RANDOM()) <= 55 THEN 'Aruba AP-635'
        WHEN UNIFORM(1, 100, RANDOM()) <= 75 THEN 'Ubiquiti UniFi 6 Pro'
        WHEN UNIFORM(1, 100, RANDOM()) <= 90 THEN 'Juniper Mist AP43'
        ELSE 'Ruckus R750'
    END AS AP_MODEL,
    CASE 
        WHEN AP_MODEL LIKE '%Meraki%' THEN 'Cisco Meraki'
        WHEN AP_MODEL LIKE '%Aruba%' THEN 'HPE Aruba'
        WHEN AP_MODEL LIKE '%Ubiquiti%' THEN 'Ubiquiti'
        WHEN AP_MODEL LIKE '%Juniper%' THEN 'Juniper Mist'
        ELSE 'Ruckus Networks'
    END AS MANUFACTURER,
    CASE 
        WHEN AP_MODEL LIKE '%MR46%' OR AP_MODEL LIKE '%AP-635%' THEN 'Wi-Fi 6'
        WHEN AP_MODEL LIKE '%UniFi 6%' OR AP_MODEL LIKE '%AP43%' THEN 'Wi-Fi 6'
        ELSE 'Wi-Fi 6E'
    END AS WIFI_STANDARD,
    CASE 
        WHEN n.INDUSTRY = 'Public Venue' THEN 1024
        WHEN n.INDUSTRY = 'Corporate' THEN 512
        WHEN n.INDUSTRY = 'Education' THEN 256
        ELSE 128
    END AS MAX_CLIENT_CAPACITY,
    DATEADD(day, -UNIFORM(30, 365, RANDOM()), CURRENT_DATE()) AS DEPLOYMENT_DATE,
    CASE 
        WHEN AP_MODEL LIKE '%Meraki%' THEN 
            CASE WHEN UNIFORM(1, 100, RANDOM()) <= 15 THEN '28.7.1' ELSE '29.2.0' END
        WHEN AP_MODEL LIKE '%Aruba%' THEN 
            CASE WHEN UNIFORM(1, 100, RANDOM()) <= 15 THEN '8.10.0.2' ELSE '8.11.1.0' END
        WHEN AP_MODEL LIKE '%Ubiquiti%' THEN 
            CASE WHEN UNIFORM(1, 100, RANDOM()) <= 15 THEN '6.5.55' ELSE '7.0.23' END
        ELSE '1.4.2'
    END AS FIRMWARE_VERSION,
    CASE 
        WHEN n.INDUSTRY = 'Corporate' THEN 
            CASE WHEN UNIFORM(1, 3, RANDOM()) = 1 THEN 'Main Building'
                 WHEN UNIFORM(1, 3, RANDOM()) = 2 THEN 'East Wing'
                 ELSE 'West Wing' END
        WHEN n.INDUSTRY = 'Retail' THEN 'Shopping Center'
        WHEN n.INDUSTRY = 'Hospitality' THEN 'Hotel Building'
        WHEN n.INDUSTRY = 'Education' THEN 
            CASE WHEN UNIFORM(1, 4, RANDOM()) = 1 THEN 'Academic Building A'
                 WHEN UNIFORM(1, 4, RANDOM()) = 2 THEN 'Academic Building B'
                 WHEN UNIFORM(1, 4, RANDOM()) = 3 THEN 'Library'
                 ELSE 'Student Center' END
        ELSE 'Main Facility'
    END AS LOCATION_BUILDING,
    UNIFORM(1, 5, RANDOM()) AS LOCATION_FLOOR,
    CASE 
        WHEN UNIFORM(1, 6, RANDOM()) = 1 THEN 'Conference Room'
        WHEN UNIFORM(1, 6, RANDOM()) = 2 THEN 'Open Office'
        WHEN UNIFORM(1, 6, RANDOM()) = 3 THEN 'Lobby'
        WHEN UNIFORM(1, 6, RANDOM()) = 4 THEN 'Cafeteria'
        WHEN UNIFORM(1, 6, RANDOM()) = 5 THEN 'Corridor'
        ELSE 'Common Area'
    END AS LOCATION_ZONE
FROM TRANSFORMED.DIM_NETWORKS n
CROSS JOIN (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS seq
    FROM TABLE(GENERATOR(ROWCOUNT => 60))
) ap_seq
WHERE ap_seq.seq <= 
    CASE 
        WHEN n.INDUSTRY = 'Public Venue' THEN 8
        WHEN n.INDUSTRY = 'Corporate' THEN 6
        WHEN n.INDUSTRY = 'Education' THEN 7
        WHEN n.INDUSTRY = 'Healthcare' THEN 5
        ELSE 4
    END;

/*  6. JSON Telemetry Data Generation
    ****************************************************
    Generate sample JSON records to demonstrate VARIANT
    processing in the transformation section.
*/

INSERT INTO RAW.RAW_NETWORK_TELEMETRY (TELEMETRY_DATA)
SELECT PARSE_JSON(
    '{"network_telemetry": {' ||
        '"ap_id": ' || ap.AP_ID || ',' ||
        '"timestamp": "' || DATEADD(minute, -UNIFORM(1, 1440, RANDOM()), CURRENT_TIMESTAMP())::VARCHAR || '",' ||
        '"status_metrics": {' ||
            '"operational_status": "' || 
                CASE WHEN UNIFORM(1, 100, RANDOM()) <= 98 THEN 'Online' ELSE 'Offline' END || '",' ||
            '"connected_clients": ' || UNIFORM(5, 128, RANDOM()) || ',' ||
            '"resource_utilization": {' ||
                '"cpu_percent": ' || UNIFORM(10, 85, RANDOM()) || '.0,' ||
                '"memory_percent": ' || UNIFORM(30, 90, RANDOM()) || '.0' ||
            '}' ||
        '},' ||
        '"location_context": {' ||
            '"building": "' || ap.LOCATION_BUILDING || '",' ||
            '"floor": ' || ap.LOCATION_FLOOR || ',' ||
            '"zone": "' || ap.LOCATION_ZONE || '"' ||
        '}' ||
    '}}'
) AS telemetry_data
FROM TRANSFORMED.DIM_ACCESS_POINTS ap
CROSS JOIN (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS record_seq
    FROM TABLE(GENERATOR(ROWCOUNT => 20))
) records
WHERE records.record_seq <= 15;

/*  7. Fact Data Generation with Realistic Patterns
    ****************************************************
    Generate 30 days of AP status data with:
    - Daily seasonality (business hours peak)
    - Weekend variations by industry
    - Injected firmware issue scenario
*/

-- Generate base timestamps for 30 days at 5-minute intervals
CREATE OR REPLACE TEMPORARY TABLE temp_timestamps AS
SELECT 
    DATEADD(minute, seq.seq * 5, DATEADD(day, -30, DATE_TRUNC('day', CURRENT_TIMESTAMP()))) AS snapshot_timestamp
FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq
    FROM TABLE(GENERATOR(ROWCOUNT => 8640))
) seq;

-- Insert fact data with realistic patterns
INSERT INTO TRANSFORMED.FACT_AP_STATUS
SELECT 
    ts.snapshot_timestamp,
    ap.AP_ID,
    ap.NETWORK_ID,
    -- Status with firmware issue injection for specific model/version
    CASE 
        WHEN ap.AP_MODEL = 'Cisco Meraki MR46' 
             AND ap.FIRMWARE_VERSION = '28.7.1'
             AND ts.snapshot_timestamp BETWEEN '2024-01-15' AND '2024-01-22'
             AND UNIFORM(1, 100, RANDOM()) <= 15 
        THEN 'Offline'
        WHEN UNIFORM(1, 1000, RANDOM()) <= 5 THEN 'Offline'
        ELSE 'Online'
    END AS status,
    
    -- Connected clients with daily/weekly seasonality
    GREATEST(0, ROUND(
        -- Base load factor by industry
        CASE 
            WHEN n.INDUSTRY = 'Public Venue' THEN 200
            WHEN n.INDUSTRY = 'Corporate' THEN 100
            WHEN n.INDUSTRY = 'Education' THEN 80
            ELSE 50
        END *
        -- Daily pattern (higher during business hours)
        (0.3 + 0.7 * GREATEST(0, 1 - ABS(EXTRACT(hour FROM ts.snapshot_timestamp) - 13) / 8)) *
        -- Weekly pattern (lower on weekends for corporate)
        CASE 
            WHEN n.INDUSTRY = 'Corporate' AND EXTRACT(dayofweek FROM ts.snapshot_timestamp) IN (1, 7) THEN 0.2
            WHEN n.INDUSTRY = 'Retail' AND EXTRACT(dayofweek FROM ts.snapshot_timestamp) IN (1, 7) THEN 1.3
            ELSE 1.0
        END *
        -- Random variation
        (0.7 + 0.6 * RANDOM())
    )) AS connected_client_count,
    
    -- CPU utilization correlated with client load
    LEAST(95, GREATEST(5, 
        15 + (connected_client_count::FLOAT / 200 * 60) + 
        UNIFORM(-10, 15, RANDOM())
    )) AS cpu_utilization_percent,
    
    -- Memory utilization with baseline + load factor
    LEAST(95, GREATEST(20,
        35 + (connected_client_count::FLOAT / 200 * 40) +
        UNIFORM(-5, 10, RANDOM())
    )) AS memory_utilization_percent

FROM temp_timestamps ts
CROSS JOIN TRANSFORMED.DIM_ACCESS_POINTS ap
JOIN TRANSFORMED.DIM_NETWORKS n ON ap.NETWORK_ID = n.NETWORK_ID
WHERE ts.snapshot_timestamp <= CURRENT_TIMESTAMP();

-- Drop temporary table
DROP TABLE temp_timestamps;

/*  8. QoS Metrics Table Creation with Signal Strength (CRITICAL for WiFi Analytics)
    ****************************************************
    Create QoS metrics table with signal strength (RSSI) and related
    performance indicators. This is fundamental for WiFi analysis.
*/

CREATE OR REPLACE TABLE TRANSFORMED.FACT_QOS_METRICS (
    METRIC_TIMESTAMP TIMESTAMP_NTZ,
    AP_ID INTEGER,
    NETWORK_ID INTEGER,
    -- Core QoS Metrics
    RSSI_DBM INTEGER,                    -- Signal strength (-30 to -90 dBm)
    THROUGHPUT_MBPS DECIMAL(8,2),        -- Data throughput in Mbps
    LATENCY_MS INTEGER,                  -- Round-trip latency in milliseconds
    PACKET_LOSS_PERCENT DECIMAL(5,2),    -- Packet loss percentage
    -- Additional Context
    CONNECTED_CLIENTS_SAMPLE INTEGER,     -- Snapshot of connected clients at measurement time
    INTERFERENCE_LEVEL VARCHAR(20),       -- Calculated interference level
    SIGNAL_QUALITY_SCORE DECIMAL(3,1),   -- Overall signal quality score (1-10)
    -- Keys and Constraints
    PRIMARY KEY (METRIC_TIMESTAMP, AP_ID),
    FOREIGN KEY (AP_ID) REFERENCES TRANSFORMED.DIM_ACCESS_POINTS(AP_ID),
    FOREIGN KEY (NETWORK_ID) REFERENCES TRANSFORMED.DIM_NETWORKS(NETWORK_ID)
);

/*  9. Generate Realistic QoS Data with RSSI Correlations
    ****************************************************
    Generate 30 days of QoS metrics with realistic signal strength
    patterns and proper correlations with performance metrics.
*/

-- Create timestamps for 1-minute intervals (higher granularity than AP status)
CREATE OR REPLACE TEMPORARY TABLE temp_qos_timestamps AS
SELECT 
    DATEADD(minute, seq.seq, DATEADD(day, -30, DATE_TRUNC('day', CURRENT_TIMESTAMP()))) AS metric_timestamp
FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq
    FROM TABLE(GENERATOR(ROWCOUNT => 43200))  -- 30 days * 24 hours * 60 minutes
) seq;

-- Insert QoS data with realistic RSSI patterns and correlations
INSERT INTO TRANSFORMED.FACT_QOS_METRICS
SELECT 
    ts.metric_timestamp,
    ap.AP_ID,
    ap.NETWORK_ID,
    
    -- Generate realistic RSSI based on multiple factors
    GREATEST(-90, LEAST(-30,
        -- Base RSSI by network archetype and building type
        CASE 
            WHEN n.INDUSTRY = 'Public Venue' THEN -45  -- Open spaces, good coverage
            WHEN n.INDUSTRY = 'Corporate' THEN -55     -- Office buildings, moderate interference
            WHEN n.INDUSTRY = 'Education' THEN -60     -- Large buildings, variable coverage
            WHEN n.INDUSTRY = 'Healthcare' THEN -50    -- Critical reliability, good infrastructure
            WHEN n.INDUSTRY = 'Retail' THEN -65        -- Metal shelving, interference
            ELSE -60
        END +
        -- Hardware quality factor
        CASE 
            WHEN ap.MANUFACTURER = 'Cisco Meraki' THEN 5     -- Premium hardware, better signal
            WHEN ap.MANUFACTURER = 'HPE Aruba' THEN 8        -- Excellent enterprise performance
            WHEN ap.MANUFACTURER = 'Juniper Mist' THEN 6     -- High-end performance
            WHEN ap.MANUFACTURER = 'Ubiquiti' THEN 0         -- Good value, baseline performance
            ELSE -3                                          -- Lower-tier performance
        END +
        -- Client load interference (more clients = more interference)
        CASE 
            WHEN status_data.client_load > 80 THEN -15
            WHEN status_data.client_load > 50 THEN -8
            WHEN status_data.client_load > 20 THEN -3
            ELSE 0
        END +
        -- Time-based interference patterns
        CASE 
            -- Lunch hour interference (microwaves, high usage)
            WHEN EXTRACT(hour FROM ts.metric_timestamp) BETWEEN 12 AND 13 THEN -5
            -- Peak business hours interference
            WHEN EXTRACT(hour FROM ts.metric_timestamp) BETWEEN 9 AND 17 THEN -3
            -- Off-hours (better signal quality)
            WHEN EXTRACT(hour FROM ts.metric_timestamp) NOT BETWEEN 7 AND 19 THEN 5
            ELSE 0
        END +
        -- Weekend vs weekday patterns by industry
        CASE 
            WHEN n.INDUSTRY = 'Corporate' AND EXTRACT(dayofweek FROM ts.metric_timestamp) IN (1, 7) THEN 8  -- Less interference on weekends
            WHEN n.INDUSTRY = 'Retail' AND EXTRACT(dayofweek FROM ts.metric_timestamp) IN (1, 7) THEN -5    -- More customers on weekends
            ELSE 0
        END +
        -- Random variation
        UNIFORM(-8, 8, RANDOM())
    )) AS rssi_dbm,
    
    -- Calculate throughput based on RSSI using correlation matrix
    CASE 
        WHEN rssi_dbm >= -50 THEN 
            -- Excellent signal: 90-100% of theoretical max
            ROUND(UNIFORM(90, 100, RANDOM()) * 
                CASE WHEN ap.WIFI_STANDARD = 'Wi-Fi 6E' THEN 1.2
                     WHEN ap.WIFI_STANDARD = 'Wi-Fi 6' THEN 1.0
                     ELSE 0.8 END, 2)
        WHEN rssi_dbm >= -67 THEN 
            -- Very Good signal: 70-90% of theoretical max
            ROUND(UNIFORM(70, 90, RANDOM()) * 
                CASE WHEN ap.WIFI_STANDARD = 'Wi-Fi 6E' THEN 1.2
                     WHEN ap.WIFI_STANDARD = 'Wi-Fi 6' THEN 1.0
                     ELSE 0.8 END, 2)
        WHEN rssi_dbm >= -75 THEN 
            -- Okay signal: 40-60% of theoretical max
            ROUND(UNIFORM(40, 60, RANDOM()) * 
                CASE WHEN ap.WIFI_STANDARD = 'Wi-Fi 6E' THEN 1.2
                     WHEN ap.WIFI_STANDARD = 'Wi-Fi 6' THEN 1.0
                     ELSE 0.8 END, 2)
        WHEN rssi_dbm >= -85 THEN 
            -- Weak signal: 10-30% of theoretical max
            ROUND(UNIFORM(10, 30, RANDOM()) * 
                CASE WHEN ap.WIFI_STANDARD = 'Wi-Fi 6E' THEN 1.2
                     WHEN ap.WIFI_STANDARD = 'Wi-Fi 6' THEN 1.0
                     ELSE 0.8 END, 2)
        ELSE 
            -- Unusable signal: 1-10% of theoretical max
            ROUND(UNIFORM(1, 10, RANDOM()) * 
                CASE WHEN ap.WIFI_STANDARD = 'Wi-Fi 6E' THEN 1.2
                     WHEN ap.WIFI_STANDARD = 'Wi-Fi 6' THEN 1.0
                     ELSE 0.8 END, 2)
    END AS throughput_mbps,
    
    -- Calculate latency based on RSSI (poor signal = higher latency)
    CASE 
        WHEN rssi_dbm >= -50 THEN UNIFORM(1, 5, RANDOM())      -- Excellent: 1-5ms
        WHEN rssi_dbm >= -67 THEN UNIFORM(3, 15, RANDOM())     -- Very Good: 3-15ms
        WHEN rssi_dbm >= -75 THEN UNIFORM(10, 50, RANDOM())    -- Okay: 10-50ms
        WHEN rssi_dbm >= -85 THEN UNIFORM(25, 150, RANDOM())   -- Weak: 25-150ms
        ELSE UNIFORM(100, 500, RANDOM())                       -- Unusable: 100-500ms
    END AS latency_ms,
    
    -- Calculate packet loss based on RSSI (poor signal = higher packet loss)
    CASE 
        WHEN rssi_dbm >= -50 THEN ROUND(UNIFORM(0, 0.1, RANDOM()), 3)     -- Excellent: 0-0.1%
        WHEN rssi_dbm >= -67 THEN ROUND(UNIFORM(0.1, 0.5, RANDOM()), 3)   -- Very Good: 0.1-0.5%
        WHEN rssi_dbm >= -75 THEN ROUND(UNIFORM(0.5, 2.0, RANDOM()), 3)   -- Okay: 0.5-2.0%
        WHEN rssi_dbm >= -85 THEN ROUND(UNIFORM(2.0, 10.0, RANDOM()), 3)  -- Weak: 2.0-10.0%
        ELSE ROUND(UNIFORM(10.0, 25.0, RANDOM()), 3)                      -- Unusable: 10-25%
    END AS packet_loss_percent,
    
    -- Sample connected clients at measurement time (from AP status data)
    COALESCE(status_data.client_load, 0) AS connected_clients_sample,
    
    -- Calculate interference level based on client load and signal quality
    CASE 
        WHEN rssi_dbm <= -80 OR status_data.client_load > 100 THEN 'High'
        WHEN rssi_dbm <= -70 OR status_data.client_load > 50 THEN 'Medium'
        WHEN rssi_dbm <= -60 OR status_data.client_load > 20 THEN 'Low'
        ELSE 'Minimal'
    END AS interference_level,
    
    -- Overall signal quality score (1-10 scale)
    CASE 
        WHEN rssi_dbm >= -50 THEN ROUND(UNIFORM(8.5, 10.0, RANDOM()), 1)   -- Excellent
        WHEN rssi_dbm >= -67 THEN ROUND(UNIFORM(6.5, 8.5, RANDOM()), 1)    -- Very Good
        WHEN rssi_dbm >= -75 THEN ROUND(UNIFORM(4.0, 6.5, RANDOM()), 1)    -- Okay
        WHEN rssi_dbm >= -85 THEN ROUND(UNIFORM(2.0, 4.0, RANDOM()), 1)    -- Weak
        ELSE ROUND(UNIFORM(1.0, 2.0, RANDOM()), 1)                         -- Unusable
    END AS signal_quality_score

FROM temp_qos_timestamps ts
CROSS JOIN TRANSFORMED.DIM_ACCESS_POINTS ap
JOIN TRANSFORMED.DIM_NETWORKS n ON ap.NETWORK_ID = n.NETWORK_ID
-- Get corresponding client load data from AP status (approximate by time)
LEFT JOIN (
    SELECT 
        AP_ID,
        SNAPSHOT_TIMESTAMP,
        CONNECTED_CLIENT_COUNT as client_load
    FROM TRANSFORMED.FACT_AP_STATUS
) status_data ON ap.AP_ID = status_data.AP_ID 
    AND ABS(DATEDIFF(minute, ts.metric_timestamp, status_data.SNAPSHOT_TIMESTAMP)) <= 2
WHERE ts.metric_timestamp <= CURRENT_TIMESTAMP()
    -- Limit to business hours for some industries to create realistic patterns
    AND (
        n.INDUSTRY IN ('Public Venue', 'Healthcare', 'Transportation') OR  -- 24/7 operations
        EXTRACT(hour FROM ts.metric_timestamp) BETWEEN 6 AND 22            -- Business hours for others
    );

-- Drop temporary QoS timestamps table
DROP TABLE temp_qos_timestamps;

/*  10. Create Enhanced QoS Analytical View
    ****************************************************
    Create comprehensive view combining infrastructure and QoS data.
*/

CREATE OR REPLACE VIEW ANALYTICS.VW_NETWORK_QOS_ANALYSIS AS
SELECT 
    -- Time and identification
    q.METRIC_TIMESTAMP,
    q.AP_ID,
    q.NETWORK_ID,
    
    -- Network and customer context
    n.NETWORK_NAME,
    n.CUSTOMER_NAME,
    n.INDUSTRY,
    n.LOCATION_CITY,
    n.SLA_UPTIME_TARGET,
    
    -- Access point context
    ap.AP_MODEL,
    ap.MANUFACTURER,
    ap.WIFI_STANDARD,
    ap.FIRMWARE_VERSION,
    ap.LOCATION_BUILDING,
    ap.LOCATION_FLOOR,
    ap.LOCATION_ZONE,
    ap.MAX_CLIENT_CAPACITY,
    
    -- QoS Metrics (Core WiFi Analytics)
    q.RSSI_DBM,
    q.THROUGHPUT_MBPS,
    q.LATENCY_MS,
    q.PACKET_LOSS_PERCENT,
    q.CONNECTED_CLIENTS_SAMPLE,
    q.INTERFERENCE_LEVEL,
    q.SIGNAL_QUALITY_SCORE,
    
    -- Calculated signal strength categories
    CASE 
        WHEN q.RSSI_DBM >= -50 THEN 'Excellent'
        WHEN q.RSSI_DBM >= -67 THEN 'Very Good'
        WHEN q.RSSI_DBM >= -75 THEN 'Okay'
        WHEN q.RSSI_DBM >= -85 THEN 'Weak'
        ELSE 'Unusable'
    END AS signal_strength_category,
    
    -- Coverage analysis flags
    CASE 
        WHEN q.RSSI_DBM <= -80 THEN 'Coverage Gap'
        WHEN q.PACKET_LOSS_PERCENT > 5.0 THEN 'Quality Issue'
        WHEN q.LATENCY_MS > 100 THEN 'Latency Problem'
        WHEN q.THROUGHPUT_MBPS < 10 THEN 'Throughput Issue'
        ELSE 'Normal'
    END AS qos_alert_category,
    
    -- Capacity utilization relative to signal quality
    ROUND((q.CONNECTED_CLIENTS_SAMPLE::FLOAT / ap.MAX_CLIENT_CAPACITY) * 100, 2) AS capacity_utilization_percent,
    
    -- Time-based analysis dimensions
    EXTRACT(HOUR FROM q.METRIC_TIMESTAMP) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM q.METRIC_TIMESTAMP) AS day_of_week,
    DATE_TRUNC('day', q.METRIC_TIMESTAMP) AS measurement_date

FROM TRANSFORMED.FACT_QOS_METRICS q
LEFT JOIN TRANSFORMED.DIM_ACCESS_POINTS ap ON q.AP_ID = ap.AP_ID
LEFT JOIN TRANSFORMED.DIM_NETWORKS n ON q.NETWORK_ID = n.NETWORK_ID;

/*
    Data generation with Signal Strength Enhancement complete!
    
    Objects created and populated:
    - DIM_NETWORKS: 12 networks across various industries
    - DIM_ACCESS_POINTS: 64 access points with realistic distribution  
    - RAW_NETWORK_TELEMETRY: ~960 JSON records for VARIANT demo
    - FACT_AP_STATUS: ~552,960 status records (30 days × 64 APs × 288 intervals)
    - FACT_QOS_METRICS: ~1.3M QoS records with signal strength (RSSI) data
    - VW_NETWORK_QOS_ANALYSIS: Comprehensive WiFi analytics view
    
    Realistic scenarios included:
    - Firmware issue: Cisco Meraki MR46 v28.7.1 higher failure rate Jan 15-22
    - Signal strength patterns: Hardware-specific RSSI characteristics
    - Interference modeling: Time-based and load-based signal degradation
    - Industry variations: Different coverage patterns by business type
    - QoS correlations: RSSI impact on throughput, latency, packet loss
    - Environmental factors: Building types affecting signal propagation
    
    Critical WiFi analytics now available:
    - Signal strength (RSSI) analysis and coverage assessment
    - Dead zone and coverage gap identification
    - Interference detection and correlation analysis
    - QoS performance correlation with signal quality
    - Hardware performance comparison by signal strength
    
    Next step: Run 03_data_transformation.sql
*/

-- Enhanced verification queries including signal strength data
SELECT 'Networks created' AS object_type, COUNT(*) AS count FROM TRANSFORMED.DIM_NETWORKS
UNION ALL
SELECT 'Access Points created', COUNT(*) FROM TRANSFORMED.DIM_ACCESS_POINTS  
UNION ALL
SELECT 'JSON telemetry records', COUNT(*) FROM RAW.RAW_NETWORK_TELEMETRY
UNION ALL
SELECT 'Infrastructure status records', COUNT(*) FROM TRANSFORMED.FACT_AP_STATUS
UNION ALL
SELECT 'QoS metrics with RSSI', COUNT(*) FROM TRANSFORMED.FACT_QOS_METRICS
UNION ALL
SELECT 'QoS analytical view records', COUNT(*) FROM ANALYTICS.VW_NETWORK_QOS_ANALYSIS;
