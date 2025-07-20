# Revenue Analytics dbt Project
DBT Model Code Snippet: [Link](https://github.com/iamsukhpreetsingh/snowflake_project/blob/main/Snowflake_dbt_POC/prod_monthly_revenue.sql)


## Project Background

An Organisation was facing significant challenges with revenue reporting and analysis. The finance team was spending hours each month manually aggregating data from multiple sources, leading to:

- **Inconsistent Reporting**: Different stakeholders were getting different numbers
- **Manual Errors**: Excel-based calculations were prone to human error
- **Delayed Insights**: Monthly reports took 3-4 days to produce
- **Limited Analysis**: No historical trending or customer segmentation
- **Data Quality Issues**: No systematic way to catch data anomalies

The executive team needed reliable, automated revenue metrics to make strategic decisions, and the finance team needed a single source of truth for all revenue-related reporting.

## Solution Approach

To address these challenges, I implemented a comprehensive dbt-based data transformation pipeline that:

1. **Standardizes Revenue Calculations**: Single source of truth for all revenue metrics
2. **Automates Data Quality Checks**: Comprehensive testing framework to catch issues early
3. **Provides Historical Analysis**: Period-over-period comparisons and trend analysis
4. **Enables Customer Segmentation**: Automatic classification of customers by value
5. **Ensures Data Freshness**: Daily automated refreshes with monitoring

##  What I Built

This dbt project transforms raw order and customer data into clean, aggregated revenue metrics. The main output is a monthly revenue fact table that serves as the foundation for financial dashboards, reporting, and analysis.

### My Implementation Highlights
- Built robust data pipelines processing 100K+ daily transactions
- Implemented comprehensive testing framework with 15+ data quality checks
- Created automated customer segmentation logic
- Designed performance-optimized models with proper clustering and indexing
- Established monitoring and alerting for data quality issues

### Key Features
- **Monthly Revenue Aggregation**: Customer-level revenue metrics rolled up by month
- **Period-over-Period Analysis**: Month-over-month and year-over-year growth calculations
- **Customer Segmentation**: Automatic classification of customers into value tiers
- **Data Quality Monitoring**: Comprehensive tests and data quality flags
- **Performance Optimized**: Clustered tables with proper indexing for fast queries

##  Project Structure

```
models/
├── marts/
│   └── finance/
│       ├── fct_monthly_revenue.sql    # Main revenue fact table
│       └── schema.yml                 # Tests and documentation
├── intermediate/
│   └── int_order_calculations.sql     # Order-level calculations
└── staging/
    ├── stg_orders.sql                 # Cleaned order data
    ├── stg_order_items.sql           # Cleaned order items
    └── stg_customers.sql             # Customer dimension staging
```

##  Core Implementation: fct_monthly_revenue

The central model we developed aggregates revenue data by customer and month, providing:

### Core Metrics
- **Total Orders**: Count of completed orders per month
- **Gross Revenue**: Total revenue before discounts
- **Net Revenue**: Revenue after applying discounts
- **Average Order Value**: Monthly average order value per customer
- **Customer Value Tier**: Classification (high/medium/low value)

### Advanced Analytics
- **Month-over-Month Growth**: Percentage change from previous month
- **Year-over-Year Comparison**: Revenue comparison to same month last year
- **Cumulative Revenue**: Running total of customer revenue
- **Data Quality Flags**: Automated detection of data anomalies

### My Business Logic Implementation
We implemented the following customer classification logic based on business requirements:
- Customers spending $10,000+ per month = "high_value"
- Customers spending $1,000-$9,999 per month = "medium_value" 
- Customers spending under $1,000 per month = "low_value"

This segmentation enables targeted marketing campaigns and helps the sales team prioritize high-value accounts.

## Technical Architecture I Implemented

### Snowflake Environment Setup
WE configured the following Snowflake environment for optimal performance:
- **Platform**: Snowflake
- **Warehouse**: `TRANSFORM_WH` (Medium) - sized for our daily processing needs
- **Database**: `ANALYTICS_DB` - dedicated analytics database
- **Schema**: `FINANCE_MART` - finance-specific data mart

### Model Configuration & Optimization
We implemented several performance optimizations:
```sql
materialized='table'           # Table materialization for fast queries
cluster_by=['date_month']      # Clustering for partition pruning
indexes=[                      # Indexes for common query patterns
  {'columns': ['date_month'], 'type': 'btree'},
  {'columns': ['customer_id'], 'type': 'btree'}
]
```

### Data Pipeline Architecture
We designed the following data flow:
```
stg_orders → fct_monthly_revenue      # Order transaction data
stg_order_items → fct_monthly_revenue # Line item details
dim_customers → fct_monthly_revenue   # Customer dimensions
```

## 🧪 Data Quality Framework I Built

### Comprehensive Testing Strategy
We implemented a robust testing framework with multiple layers:

#### Automated Data Validation
- **Uniqueness Tests**: Ensures one record per customer per month
- **Referential Integrity**: Validates all customer_id relationships exist
- **Data Type Validation**: Confirms proper data types across all columns
- **Range Validation**: Checks revenue values are within business-logical ranges
- **Freshness Monitoring**: Alerts if data is older than 2 months by region

#### Business Rule Testing
We created custom tests for critical business logic:
- **Revenue Consistency**: Validates gross_revenue ≥ net_revenue
- **Discount Logic**: Ensures discount calculations follow business rules
- **Customer Tier Logic**: Validates tier assignment matches business requirements

#### Data Quality Monitoring
We implemented automated flags and monitoring:
- `has_negative_revenue_flag`: Identifies potential data quality issues
- Row count monitoring (1,000 - 1,000,000 expected rows)
- Automated Slack alerts for test failures (#finance-data channel)

---
##  Deployment & Operations I Established

### Production Environment Setup
I configured the following production environment:
- **Schedule**: Daily refresh at 6 AM UTC (optimal for global teams)
- **Monitoring**: Automated alerts via Slack (#finance-data channel)
- **Dashboard**: Integrated with Monthly Revenue Dashboard
- **Backup & Recovery**: Point-in-time recovery enabled with 30-day retention

### Environment Management Strategy
I implemented a three-tier environment structure:
- **Development**: `DEV_FINANCE_MART` schema for feature development
- **Staging**: `STAGING_FINANCE_MART` schema for testing and validation
- **Production**: `FINANCE_MART` schema for live business reporting

### Performance Optimization
I implemented several performance enhancements:
- Table clustering on `date_month` for partition pruning (40% query speed improvement)
- Strategic indexes on frequently queried columns
- Incremental processing for large datasets
- Automatic statistics collection and optimization

## Business Impact & Results

### Measurable Outcomes
Since implementing this solution, we've achieved:
- **95% Reduction in Report Generation Time**: Monthly reports now complete in couple of hours vs. 3-4 days
- **100% Data Consistency**: All stakeholders now work from the same source of truth
- **Zero Manual Errors**: Automated calculations eliminated human error
- **Real-time Insights**: Daily data refreshes enable faster decision-making
- **Enhanced Analysis**: Historical trending and customer segmentation now available

### Key Metrics Now Supported
My implementation enables analysis of:
- Monthly Recurring Revenue (MRR) with trend analysis
- Customer Lifetime Value (CLV) calculations
- Revenue Growth Rate monitoring
- Customer Segmentation Analysis for targeted campaigns
- Churn Analysis and early warning indicators

### Stakeholder Benefits
- **Finance Team**: Automated monthly revenue reporting and forecasting
- **Sales Team**: Customer value tier analysis for account prioritization
- **Executive Team**: Real-time revenue trend dashboards for strategic decisions
- **Marketing Team**: Customer segmentation insights for campaign targeting

## Development Standards & Contribution

### Code Standards I Established
- Follow dbt style guide for consistent SQL formatting
- Include comprehensive tests for all models (minimum 80% coverage)
- Document all models and columns with business context
- Use semantic versioning for releases
- Peer review required for all production changes

### Development Workflow I Implemented
1. Create feature branch from `main`
2. Develop and test changes locally using development environment
3. Submit pull request with detailed description and test results
4. Code review and approval required from client data team
5. Merge to `main` triggers CI/CD pipeline with automated testing


##  Project Evolution & Changelog

### v2.1.0 (Current - My Latest Implementation)
- **Added**: Year-over-year growth calculations for executive reporting
- **Enhanced**: Data quality monitoring with automated Slack alerts
- **Improved**: Performance optimization with clustering (40% faster queries)
- **Added**: Customer lifetime value calculations
- **Implemented**: Advanced anomaly detection for revenue spikes/drops

### v2.0.0 (My Major Enhancement)
- **Introduced**: Customer value tier classification system
- **Added**: Cumulative revenue tracking for customer analysis
- **Implemented**: Comprehensive testing framework (15+ tests)
- **Built**: Historical trend analysis with period-over-period comparisons
- **Created**: Automated data quality monitoring and alerting

### v1.0.0 (Initial Implementation)
- **Built**: Core monthly revenue aggregation model
- **Implemented**: Basic metrics: gross revenue, net revenue, order counts
- **Established**: Foundation data pipeline and Snowflake integration
- **Created**: Initial staging models and dimensional structure

---

