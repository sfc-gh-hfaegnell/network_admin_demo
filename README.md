# WiFi Analytics Snowflake Demo

## Overview & Objectives

This comprehensive demo showcases Snowflake's capabilities for WiFi network analytics, demonstrating the complete data flow from raw JSON ingestion to conversational analytics using Snowflake Intelligence. The demo features a sophisticated AI agent that adapts to different business contexts - providing strategic, operational, customer-focused, and technical insights from the same WiFi analytics data.

## Prerequisites & Setup

**Requirements:**
- Snowflake Enterprise Edition or higher (required for Semantic Views and Snowflake Intelligence)
- ACCOUNTADMIN privileges for initial setup
- Basic SQL knowledge
- Estimated completion time: 60 minutes (20 min Part 1, 40 min Part 2)

**Before You Begin:**
1. Ensure you have ACCOUNTADMIN access to your Snowflake account
2. Open Snowsight in your browser
3. Have this repository open for reference
4. Note: Enhanced demo includes signal strength (RSSI) analysis - the core of WiFi analytics

## Demo Architecture

The demo creates a simplified star schema focused on WiFi network performance:

**Core Tables (Used Throughout):**
- `DIM_NETWORKS` - 12 customer networks across diverse industries
- `DIM_ACCESS_POINTS` - 64 access points with realistic manufacturer distribution  
- `FACT_AP_STATUS` - 552,960 infrastructure performance records (30 days of data)
- `FACT_QOS_METRICS` - 1.3M+ signal strength and QoS records (1-minute intervals)

**Supporting Objects:**
- `RAW_NETWORK_TELEMETRY` - 960 JSON records for VARIANT demonstration
- `VW_AP_PERFORMANCE` - Analytical view combining JSON and dimensional data
- `VW_NETWORK_QOS_ANALYSIS` - Comprehensive view with signal strength and coverage analysis
- `NETWORK_ANALYTICS_SV` - Advanced semantic view with QoS metrics, signal strength, and business context
- Dynamic masking policies for location data privacy
- Role-based access control (NETWORK_ANALYST, EXTERNAL_ANALYST)

## Part 1: Data Foundation (20 minutes)

This section demonstrates fundamental Snowflake capabilities for data transformation and governance.

### Step 1: Environment Setup
Run the SQL worksheet: `worksheets/01_environment_setup.sql`
- Creates database, schemas, roles, and warehouses
- Sets up proper permissions and context

### Step 2: Synthetic Data Generation with Signal Strength
Run the SQL worksheet: `worksheets/02_synthetic_data_creation.sql`
- Generates realistic WiFi telemetry data with seasonal patterns
- Creates dimension tables with industry-appropriate distributions
- **Includes signal strength (RSSI) and QoS metrics** - critical for WiFi analytics
- Implements RSSI correlation matrix with throughput, latency, and packet loss
- Creates realistic interference patterns and environmental factors
- Generates 1.3M+ QoS measurements with hardware-specific signal characteristics


### Step 3: JSON Data Transformation
Run the SQL worksheet: `worksheets/03_json_transformation.sql`
- Demonstrates VARIANT data type with nested JSON
- Shows dot-notation extraction and FLATTEN operations
- Creates analytical views from semi-structured data

### Step 4: Governance & RBAC
Run the SQL worksheet: `worksheets/04_governance_rbac.sql`
- Implements dynamic masking for location data
- Demonstrates role-based access control
- Shows data privacy capabilities

## Part 2: Snowflake Intelligence (40 minutes)

This section focuses on advanced analytics and conversational AI capabilities.

### Step 5: Enhanced Semantic Views with Signal Strength
Run the SQL worksheet: `worksheets/05_semantic_views.sql`
- Creates comprehensive semantic view including QoS metrics and signal strength
- Defines relationships across infrastructure and QoS data tables
- Implements signal strength analysis with coverage and interference detection
- Includes RSSI, throughput, latency, and packet loss metrics for complete WiFi analytics
- Enables natural language queries about coverage gaps, dead zones, and signal quality

### Step 6: Snowflake Intelligence Agent Setup
Follow instructions in: `worksheets/06_snowflake_intelligence.sql`
- **Manual Configuration Required:** Create agent through Snowsight UI
- **Agent Name:** WIFI_NETWORK_ANALYTICS_ASSISTANT
- **Multi-Role Capabilities:** Strategic, Operational, Customer Success, Technical Analytics
- **Adaptive Responses:** Context-aware styling based on question type
- **Complete Demo Script:** 20-minute presentation with role-specific questions

### Step 7: Comprehensive Validation
Run the SQL worksheet: `worksheets/07_validation_queries.sql`
- Validates all data relationships and quality
- Tests semantic view functionality
- Confirms signal strength data integrity
- Provides comprehensive data quality metrics

## Sample Business Questions

Once the Snowflake Intelligence agent is deployed, you can ask questions across different business contexts:

**Strategic Executive Questions:**
- What is our overall WiFi infrastructure ROI and which technology investments should we prioritize based on signal strength performance?
- Compare total cost of ownership and coverage quality between Cisco, Aruba, and Ubiquiti deployments
- Which industry verticals have the best signal coverage and represent growth opportunities?

**Operational Management Questions:**
- Which access points have poor signal strength and require immediate attention?
- Show me areas with signal strength below -75 dBm that need coverage improvements
- What are the root causes of poor signal quality and how can we optimize coverage?

**Customer Success Questions:**
- Which customers are experiencing poor WiFi coverage and need proactive outreach?
- Compare signal strength and coverage quality across different industry types
- Show me customers with signal strength issues that could impact satisfaction

**Signal Strength & Coverage Analysis:**
- Show me a coverage heatmap analysis based on signal strength measurements
- Which buildings or zones have the weakest WiFi signal and need attention?
- How does signal strength vary throughout the day and what causes interference?
- What's the correlation between client density and signal quality degradation?

**QoS & Performance Analysis:**
- How does poor signal strength impact throughput and latency across different manufacturers?
- Which access points have packet loss issues correlated with weak signal strength?
- Analyze interference patterns and their impact on WiFi performance

## Validation & Testing

Run the SQL worksheet: `sample-queries/validation_queries.sql`
- Validates all created objects are functional
- Tests semantic view accessibility and structure with QoS metrics
- Verifies data quality and RSSI correlation accuracy
- Confirms governance controls are working
- Validates signal strength patterns and coverage analysis capabilities
- Tests interference detection and QoS correlation algorithms

## Troubleshooting Guide

**Common Issues:**

1. **Semantic View Creation Fails**
   - Ensure you have Enterprise Edition or higher
   - Verify all referenced tables exist and have data
   - Check that primary keys are properly defined

2. **Snowflake Intelligence Not Available**
   - Confirm your account has access to Cortex features
   - Verify you're in a supported region
   - Check that semantic view is properly created

3. **Permission Errors**
   - Ensure you're using ACCOUNTADMIN for setup
   - Verify role has necessary privileges on schemas
   - Check warehouse access permissions

4. **Data Generation Issues**
   - Verify sufficient warehouse compute resources
   - Check for any constraint violations in sample data
   - Ensure timestamp formats are correct

## Extension Ideas

**Advanced Scenarios:**
- Add real-time streaming data with Snowpipe
- Implement alerting with Snowflake notifications
- Create Streamlit dashboards for visualization
- Add machine learning models for predictive analytics
- Integrate with external WiFi management systems
- Create specialized agents for different business roles
- Add Cortex Search for unstructured network documentation

**Additional Data Sources:**
- Device inventory management
- Network security logs
- Customer satisfaction surveys
- Maintenance scheduling data
- Energy consumption metrics
- Support ticket conversations
- Network configuration changes

## Cleanup

To remove all demo objects, run:
```sql
DROP DATABASE IF EXISTS WIFI_ANALYTICS CASCADE;
DROP ROLE IF EXISTS NETWORK_ANALYST;
DROP WAREHOUSE IF EXISTS WIFI_ANALYTICS_WH;
```

## Support

For questions or issues with this demo:
1. Check the troubleshooting guide above
2. Verify all prerequisites are met
3. Review Snowflake documentation for latest feature updates
4. Ensure proper permissions and account features are enabled

---

**Note:** This demo uses synthetic data for educational purposes. All network names, locations, and performance metrics are generated for demonstration and do not represent real network infrastructure.
