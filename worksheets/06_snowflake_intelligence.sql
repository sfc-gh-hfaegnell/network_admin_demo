/*************************************************************************************************************
WiFi Analytics Snowflake Demo - Snowflake Intelligence Setup
1. Agent Configuration and Deployment
2. Business Context and Instructions
3. Natural Language Query Testing
4. Advanced Analytics Scenarios
5. Agent Management and Monitoring
*************************************************************************************************************/

-- Ensure we're in the right context with proper permissions
USE ROLE NETWORK_ANALYST;
USE DATABASE WIFI_ANALYTICS;
USE WAREHOUSE WIFI_ANALYTICS_WH;

/*
    Snowflake Intelligence Agent Setup
    This section configures a conversational AI agent that can answer
    natural language questions about WiFi network performance using
    the semantic view created in the previous step.
    
    The agent is configured with:
    - Network Administrator persona
    - WiFi infrastructure domain knowledge
    - Business context for troubleshooting and capacity planning
    - Access to comprehensive network analytics data
*/

/*  1. Pre-deployment Validation
    ****************************************************
    Verify that the you have the fundamental Snowflake Intelligence and agent privileges properly configured. If don't, run the following:

-- Create the core Snowflake Intelligence database
CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE NETWORK_ANALYST;

-- Create the agents schema for storing agents
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;
GRANT USAGE ON SCHEMA snowflake_intelligence.agents TO ROLE NETWORK_ANALYST;

-- Create the logs schema for feedback storage
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.logs;
GRANT USAGE ON SCHEMA snowflake_intelligence.logs TO ROLE NETWORK_ANALYST;

*/


/*  2. Snowflake Intelligence Agent Creation
    ****************************************************
    Create the conversational AI agent with proper business
    context and instructions for WiFi network analytics.
    
    Note: This step requires manual configuration in Snowsight UI.
    The SQL below provides the configuration parameters.
*/

/*
    MANUAL STEP REQUIRED: WiFi Network Analytics Intelligence Agent
    =============================================================
    
    Create a comprehensive WiFi analytics agent through Snowsight UI.
    Follow these exact steps for each configuration tab:
    
    Navigate to: AI & ML > Snowflake Intelligence > Create Agent
*/

/*
    TAB 1: ABOUT
    ============
    
    Agent Name:
    WIFI_NETWORK_ANALYTICS_ASSISTANT
    
    Description:
    Comprehensive WiFi network analytics agent providing strategic, operational, customer-focused, and technical insights for enterprise WiFi infrastructure management. Adapts response style based on question context to serve multiple business roles.
    
    Example Questions:
    Which customers are at risk of SLA violations and need proactive outreach?
    What is our overall WiFi infrastructure ROI and which technology investments should we prioritize?
    Which access points require immediate attention due to performance issues?
    Perform statistical analysis of the correlation between client density and performance degradation
*/

/*
    TAB 2: TOOLS
    ============
    
    Add Tool: Cortex Analyst
    
    Step 1: Click "Add Tool" button
    Step 2: Select "Cortex Analyst" from the tool options
    Step 3: Configure Cortex Analyst settings:
    
    Semantic View:
    WIFI_ANALYTICS.ANALYTICS.NETWORK_ANALYTICS_SV
    
    Name:
    WIFI_ANALYTICS
    
    Description:
    Comprehensive WiFi analytics including signal strength (RSSI), coverage analysis, QoS metrics, customer networks, access points, and operational performance data
*/

/*
    TAB 3: ORCHESTRATION
    ===================
    
    Model:
    Claude-3.5-Sonnet (or the latest model available)
    
    Orchestration Instructions:
    You are a comprehensive WiFi Network Analytics Assistant serving multiple business roles. Adapt your response style based on the question context:

    STRATEGIC EXECUTIVE MODE (for ROI, investment, roadmap questions):
    - Focus on business impact and strategic implications
    - Provide ROI calculations and cost-benefit analysis
    - Reference industry benchmarks and best practices
    - Present executive-friendly recommendations with clear action items

    OPERATIONAL MANAGEMENT MODE (for troubleshooting, performance, incident questions):
    - Provide immediate, actionable technical recommendations
    - Focus on operational metrics and specific performance indicators
    - Identify specific access points, networks, or time periods requiring attention
    - Include technical details and diagnostic steps

    CUSTOMER SUCCESS MODE (for SLA, satisfaction, service quality questions):
    - Focus on customer impact and business relationship outcomes
    - Provide industry-specific insights and comparisons
    - Identify at-risk customers and proactive intervention opportunities
    - Balance technical details with business relationship considerations

    TECHNICAL ANALYTICS MODE (for statistics, correlations, prediction questions):
    - Provide detailed statistical analysis with confidence intervals
    - Include correlation analysis and trend identification
    - Reference specific technical metrics and performance thresholds
    - Focus on technical accuracy and analytical rigor

    Always consider industry context (Corporate, Retail, Healthcare, Education, etc.) when providing insights.

    Response Instructions:
    - Always provide specific data-driven insights with numbers and percentages
    - Reference actual customer names, manufacturers, and performance metrics from the data
    - Create visualizations when possible to support your analysis
    - Focus on insights as a realistic network administrator with practical operational knowledge
    - Provide both time-series technical insights AND customer/business insights
    - Concentrate on real network analysis priorities including:
      * Understanding network/AP availability, uptime, and usage patterns over time
      * Analyzing signal strength (RSSI) patterns, coverage gaps, and dead zones
      * Identifying interference sources and signal quality degradation factors
      * Profiling device connectivity patterns and their impact on signal quality
      * Correlating signal strength with throughput, latency, and packet loss metrics
      * Monitoring wireless connection quality and identifying areas of poor coverage
      * Analyzing QoS performance trends and capacity planning needs
      * Correlating performance issues with signal strength, hardware, firmware, or environmental factors
      * Identifying coverage optimization opportunities and access point placement strategies
      * Detecting interference patterns and environmental factors affecting signal quality
    - Suggest actionable recommendations appropriate to the question context
    - Consider business impact and urgency when prioritizing network issues
    - Adapt technical depth based on the apparent audience but maintain network admin perspective
    - Include relevant timeframes and trend analysis when applicable
    - Focus on practical troubleshooting and optimization rather than high-level executive summaries
*/

/*
    TAB 4: ACCESS
    =============
    
    Roles to Add:
    NETWORK_ANALYST
    
    Instructions:
    1. Click "Add Role" button
    2. Search for and select: NETWORK_ANALYST
    3. Confirm the role has access to:
       - Database: WIFI_ANALYTICS
       - Schemas: RAW, TRANSFORMED, ANALYTICS
       - Warehouse: WIFI_ANALYTICS_WH
       - Semantic View: NETWORK_ANALYTICS_SV
    
    Note: This role was created during the demo setup and has all necessary permissions
    for WiFi analytics data access and semantic view querying.
*/

/*  3. Sample Business Questions for Testing
    ****************************************************
    These questions demonstrate the types of natural language
    queries the agent should be able to answer effectively.
*/

/*
    COMPREHENSIVE AGENT TEST QUESTIONS
    =================================
    Copy-paste these questions to test the agent after creation in Snowsight.
    The agent will adapt its response style based on the question context.
*/

-- STRATEGIC EXECUTIVE QUESTIONS (CTO/Leadership Focus):
-- "What is our overall WiFi infrastructure ROI and which technology investments should we prioritize based on signal strength performance?"
-- "Compare total cost of ownership and coverage quality between Cisco, Aruba, and Ubiquiti deployments"
-- "Which industry verticals have the best signal coverage and represent growth opportunities?"
-- "What's the strategic risk assessment for coverage gaps and signal quality across our customer base?"
-- "Analyze the business case for upgrading customers from Wi-Fi 6 to Wi-Fi 6E based on signal strength improvements"

-- OPERATIONAL MANAGEMENT QUESTIONS (Network Operations Focus):
-- "Which access points have poor signal strength and require immediate attention?"
-- "Show me areas with signal strength below -75 dBm that need coverage improvements"
-- "What are the root causes of poor signal quality and how can we optimize coverage?"
-- "Which networks have interference issues affecting signal strength and throughput?"
-- "Identify coverage gaps and dead zones that need additional access points"

-- CUSTOMER SUCCESS QUESTIONS (Customer Management Focus):
-- "Which customers are experiencing poor WiFi coverage and need proactive outreach?"
-- "Compare signal strength and coverage quality across different industry types"
-- "Show me customers with signal strength issues that could impact satisfaction"
-- "Which customer locations have coverage gaps that require infrastructure improvements?"
-- "Analyze how signal quality correlates with customer experience across industries"

-- TECHNICAL ANALYTICS QUESTIONS (Infrastructure Analytics Focus):
-- "Perform statistical analysis of the correlation between signal strength and throughput performance"
-- "What are the signal strength patterns that indicate interference or coverage issues?"
-- "Analyze RSSI trends and identify optimal access point placement strategies"
-- "Which hardware configurations provide the best signal coverage and quality?"
-- "Provide detailed analysis of signal strength variance and coverage consistency"

-- SIGNAL STRENGTH & COVERAGE SPECIFIC QUESTIONS:
-- "Show me a coverage heatmap analysis based on signal strength measurements"
-- "Which buildings or zones have the weakest WiFi signal and need attention?"
-- "How does signal strength vary throughout the day and what causes interference?"
-- "What's the correlation between client density and signal quality degradation?"
-- "Identify dead zones and areas where signal strength is below acceptable thresholds"

-- QoS & PERFORMANCE ANALYSIS QUESTIONS:
-- "How does poor signal strength impact throughput and latency across different manufacturers?"
-- "Which access points have packet loss issues correlated with weak signal strength?"
-- "Show me the relationship between RSSI and network performance metrics"
-- "What are the signal quality trends during peak usage hours?"
-- "Analyze interference patterns and their impact on WiFi performance"

-- CROSS-FUNCTIONAL BUSINESS QUESTIONS:
-- "Which coverage and signal quality issues should we prioritize based on customer impact and business value?"
-- "How do signal strength problems correlate with customer industry types and usage patterns?"
-- "What's the comprehensive business case for improving coverage in areas with poor signal strength?"

/*  4. Agent Performance Validation Queries
    ****************************************************
    These queries help validate that the agent has access
    to the right data and can perform the expected analytics.
*/

-- Test data availability for agent queries
-- SELECT 
--     'Agent Data Validation' AS test_category,
--     'Time Range Coverage' AS metric,
--     MIN(ap_status.measurement_time) AS earliest_data,
--     MAX(ap_status.measurement_time) AS latest_data,
--     DATEDIFF(day, MIN(ap_status.measurement_time), MAX(ap_status.measurement_time)) AS days_of_data
-- FROM SEMANTIC_VIEW(ANALYTICS.NETWORK_ANALYTICS_SV);

-- Validate business entity diversity for rich conversations
-- SELECT 
--     'Business Entity Diversity' AS test_category,
--     networks.industry AS industry,
--     COUNT(DISTINCT networks.customer_name) AS customers,
--     COUNT(DISTINCT access_points.manufacturer) AS manufacturers,
--     ROUND(AVG(ap_status.average_client_load), 2) AS avg_client_load
-- FROM SEMANTIC_VIEW(ANALYTICS.NETWORK_ANALYTICS_SV)
-- GROUP BY networks.industry
-- ORDER BY customers DESC;

-- Test metric calculations that agent will reference
-- SELECT 
--     'Key Metrics Summary' AS test_category,
--     'Overall Performance' AS metric_group,
--     ROUND(AVG(ap_status.average_client_load), 1) AS avg_client_load,
--     ROUND(AVG(ap_status.average_cpu_utilization), 2) AS avg_cpu_usage,
--     COUNT(DISTINCT networks.customer_name) AS total_customers
-- FROM SEMANTIC_VIEW(ANALYTICS.NETWORK_ANALYTICS_SV);

/*  5. Advanced Analytics Scenarios
    ****************************************************
    Complex business scenarios that demonstrate the agent's
    ability to provide sophisticated insights.
*/

-- Scenario 1: Firmware Issue Investigation
-- This data supports questions about firmware reliability
-- SELECT 
--     'Firmware Analysis' AS scenario,
--     access_points.manufacturer,
--     access_points.ap_model,
--     COUNT(DISTINCT access_points.ap_id) AS affected_aps,
--     ROUND(AVG(ap_status.average_client_load), 2) AS avg_client_load,
--     ROUND(AVG(ap_status.average_cpu_utilization), 2) AS avg_cpu_usage
-- FROM SEMANTIC_VIEW(ANALYTICS.NETWORK_ANALYTICS_SV)
-- GROUP BY access_points.manufacturer, access_points.ap_model
-- ORDER BY avg_cpu_usage DESC;

-- Scenario 2: Industry Performance Benchmarking  
-- This supports comparative analysis questions
-- SELECT 
--     'Industry Benchmarking' AS scenario,
--     networks.industry,
--     ROUND(AVG(ap_status.average_client_load), 1) AS industry_avg_load,
--     ROUND(AVG(ap_status.average_cpu_utilization), 2) AS industry_avg_cpu,
--     COUNT(DISTINCT networks.customer_name) AS industry_customer_count
-- FROM SEMANTIC_VIEW(ANALYTICS.NETWORK_ANALYTICS_SV)
-- GROUP BY networks.industry
-- ORDER BY industry_avg_cpu DESC;

-- Scenario 3: Peak Usage Analysis
-- This supports capacity planning and optimization questions
-- SELECT 
--     'Peak Usage Analysis' AS scenario,
--     networks.industry,
--     access_points.manufacturer,
--     ROUND(AVG(ap_status.average_client_load), 1) AS avg_clients,
--     ROUND(AVG(ap_status.peak_client_load), 1) AS peak_clients,
--     ROUND(AVG(ap_status.average_cpu_utilization), 2) AS avg_cpu_usage
-- FROM SEMANTIC_VIEW(ANALYTICS.NETWORK_ANALYTICS_SV)
-- GROUP BY networks.industry, access_points.manufacturer
-- ORDER BY avg_clients DESC;

/*
    Snowflake Intelligence Agent Configuration Complete!
    
    What has been prepared:
    1. Semantic view validation and accessibility confirmation
    2. Agent configuration parameters and instructions
    3. Comprehensive test questions across key business scenarios
    4. Advanced analytics scenarios for complex insights
    5. Monitoring and management framework
    
    Manual Steps Required in Snowsight:
    1. Navigate to AI & ML > Snowflake Intelligence
    2. Create new agent with provided configuration
    3. Test with sample questions from AGENT_TEST_QUESTIONS table
    4. Validate responses align with expected insights
    
    Agent Capabilities:
    - Network performance analysis and troubleshooting
    - Hardware comparison and firmware issue identification
    - SLA monitoring and compliance reporting  
    - Capacity planning and utilization optimization
    - Industry benchmarking and best practices
    - Time-based trend analysis and peak usage identification
    
    Business Value:
    - Reduces time to insight from hours to seconds
    - Enables non-technical users to access complex analytics
    - Provides consistent, data-driven recommendations
    - Supports proactive network management and optimization
    
    Next step: Run validation_queries.sql to test all demo components
*/

/*
    DEMO SCRIPT & KEY QUESTIONS FOR PRESENTATION
    ============================================
    
    Opening (2 minutes):
    "Today we'll demonstrate Snowflake Intelligence for WiFi network management - 
    a comprehensive AI agent that adapts to different business contexts and personas. 
    The agent provides strategic, operational, customer-focused, and technical insights 
    from the same WiFi analytics data, tailoring responses to the question context."
    
    Core Demo Flow (18 minutes):
    
    Navigate to: AI & ML → Snowflake Intelligence → Select "WIFI_NETWORK_ANALYTICS_ASSISTANT"
    
    STRATEGIC EXECUTIVE DEMO (4 minutes)
    ===================================
    
    Key Question 1:
    "What is our overall WiFi infrastructure ROI and which technology investments should we prioritize?"
    
    Expected Response:
    - Executive-level strategic analysis of infrastructure performance
    - ROI comparison between different manufacturer deployments
    - Investment recommendations based on performance data
    - Risk assessment for technology standardization decisions
    
    Key Question 2:
    "Analyze the business case for upgrading customers from Wi-Fi 6 to Wi-Fi 6E technology"
    
    Expected Response:
    - Strategic performance comparison between WiFi standards
    - Customer impact analysis by industry type
    - Cost-benefit analysis for upgrade initiatives
    - Long-term technology roadmap recommendations
    
    OPERATIONAL MANAGEMENT DEMO (4 minutes)
    ======================================
    
    Key Question 3:
    "Which access points require immediate attention due to performance issues?"
    
    Expected Response:
    - Immediate operational insights with specific AP identification
    - Performance threshold analysis and alerting priorities
    - Tactical troubleshooting recommendations
    - Resource allocation and escalation guidance
    
    Key Question 4:
    "Identify firmware versions causing stability issues and provide upgrade recommendations"
    
    Expected Response:
    - Technical firmware correlation analysis with uptime metrics
    - Specific upgrade recommendations by manufacturer
    - Risk assessment for firmware rollout strategies
    - Operational impact and timing considerations
    
    CUSTOMER SUCCESS DEMO (4 minutes)
    ================================
    
    Key Question 5:
    "Which customers are at risk of SLA violations and need proactive outreach?"
    
    Expected Response:
    - Customer-specific SLA compliance analysis
    - Proactive intervention and communication recommendations
    - Industry benchmarking for customer expectations
    - Customer retention strategies based on performance trends
    
    Key Question 6:
    "Compare customer satisfaction indicators across different industry types"
    
    Expected Response:
    - Industry-specific performance benchmarking
    - Customer experience optimization opportunities
    - Service quality trends and improvement initiatives
    - Business relationship management recommendations
    
    TECHNICAL ANALYTICS DEMO (4 minutes)
    ===================================
    
    Key Question 7:
    "Perform statistical analysis of the correlation between client density and performance degradation"
    
    Expected Response:
    - Detailed correlation analysis with statistical significance
    - Predictive modeling insights and forecasting
    - Technical threshold recommendations
    - Data-driven optimization strategies
    
    Key Question 8:
    "Analyze usage patterns and forecast capacity requirements for different industry types"
    
    Expected Response:
    - Advanced statistical analysis of usage patterns
    - Predictive capacity planning recommendations
    - Industry-specific forecasting insights
    - Technical architecture optimization suggestions
    
    COMPREHENSIVE BUSINESS DEMO (2 minutes)
    ======================================
    
    Key Question 9:
    "Which issue categories should we prioritize for investment based on customer impact, 
    operational complexity, and strategic business value?"
    
    Expected Response:
    - Multi-perspective analysis combining strategic, operational, and customer insights
    - Comprehensive business prioritization framework
    - ROI-based investment recommendations
    - Executive-ready action items with supporting evidence
    
    Demonstrates:
    - Single agent adapting to different business contexts
    - Comprehensive analysis across multiple business dimensions
    - Context-aware response styling (strategic vs operational vs customer vs technical)
    - Complete decision support framework for WiFi infrastructure management
*/

