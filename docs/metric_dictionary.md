# Metric Dictionary

This document provides definitions and calculations for all business metrics available in the BI Assistant.

## Employee Metrics

### Headcount
- **Definition**: Total number of active employees at a point in time
- **Calculation**: Count of employees where status = 'Active'
- **Dimensions**: Department, Region, Job Title, Salary Band, Tenure Category
- **Refresh**: Real-time
- **Business Context**: Core workforce sizing metric for capacity planning and budgeting

### Attrition Rate
- **Definition**: Percentage of employees who left the company in a given period
- **Calculation**: (Number of terminations / Average headcount) × 100
- **Formula**: `COUNT(terminations) / AVG(headcount) * 100`
- **Time Period**: Monthly, Quarterly, Annually
- **Dimensions**: Department, Region, Voluntary vs Involuntary, Reason Category, Tenure
- **Benchmarks**: 
  - Low: < 10% annually
  - Moderate: 10-15% annually  
  - High: > 15% annually
- **Business Context**: Key indicator of employee satisfaction and organizational health

### Retention Rate
- **Definition**: Percentage of employees retained over a specific period
- **Calculation**: 100 - Attrition Rate
- **Time Period**: Monthly, Quarterly, Annually
- **Dimensions**: Same as Attrition Rate
- **Business Context**: Positive framing of retention performance

### New Hires
- **Definition**: Number of new employees hired in a given period
- **Calculation**: Count of employees by hire_date in specified period
- **Dimensions**: Department, Region, Job Title, Salary Band
- **Business Context**: Growth indicator and recruiting effectiveness

### Terminations
- **Definition**: Number of employees who left the company in a given period
- **Calculation**: Count of termination events by termination_date
- **Dimensions**: Department, Region, Voluntary/Involuntary, Reason Category, Tenure
- **Categories**:
  - **Voluntary**: Employee-initiated departures
  - **Involuntary**: Company-initiated departures
- **Business Context**: Understanding departure patterns and reasons

### Average Tenure
- **Definition**: Average length of employment for active employees
- **Calculation**: Mean of tenure_years for all active employees
- **Unit**: Years
- **Dimensions**: Department, Region, Job Title, Salary Band
- **Business Context**: Employee stability and experience retention

## Compensation Metrics

### Average Salary
- **Definition**: Mean salary across specified employee groups
- **Calculation**: AVG(salary) for active employees
- **Dimensions**: Department, Region, Job Title, Gender, Age Category
- **Business Context**: Compensation benchmarking and equity analysis

### Salary Band Distribution
- **Definition**: Distribution of employees across predefined salary ranges
- **Bands**:
  - Entry Level: < $50,000
  - Mid Level: $50,000 - $75,000
  - Senior Level: $75,000 - $100,000
  - Executive Level: > $100,000
- **Business Context**: Compensation structure analysis

## Demographic Metrics

### Age Distribution
- **Definition**: Distribution of employees by age categories
- **Categories**: < 25, 25-34, 35-44, 45-54, 55+
- **Business Context**: Workforce demographics and succession planning

### Tenure Distribution
- **Definition**: Distribution of employees by length of service
- **Categories**: < 1 year, 1-3 years, 3-5 years, 5-10 years, 10+ years
- **Business Context**: Experience mix and retention patterns

### Gender Distribution
- **Definition**: Distribution of employees by gender
- **Categories**: Male, Female, Other/Undisclosed
- **Business Context**: Diversity and inclusion tracking

## Regional Metrics

### Regional Headcount
- **Definition**: Employee count by geographic region
- **Regions**: North America, EMEA, APAC
- **Business Context**: Geographic distribution and regional growth

### Regional Attrition
- **Definition**: Attrition rates by geographic region
- **Business Context**: Regional retention challenges and opportunities

## Operational Metrics

### Hiring Velocity
- **Definition**: Rate of new employee additions over time
- **Calculation**: Count of hires per period (monthly/quarterly)
- **Business Context**: Recruiting effectiveness and growth pace

### Time to Fill
- **Definition**: Average time to fill open positions
- **Note**: Not available in current dataset - placeholder for future enhancement
- **Business Context**: Recruiting efficiency

### Manager Span of Control
- **Definition**: Average number of direct reports per manager
- **Calculation**: COUNT(direct_reports) / COUNT(managers)
- **Business Context**: Organizational structure and management efficiency

## Data Quality Indicators

### Data Freshness
- **Definition**: Timestamp of last data update
- **Business Context**: Ensuring metrics are current and reliable

### Completeness
- **Definition**: Percentage of records with complete required fields
- **Business Context**: Data quality monitoring

## Usage Guidelines

### Metric Interpretation
- Always consider the time period when analyzing metrics
- Compare metrics across similar time periods for meaningful analysis
- Consider seasonal patterns in hiring and attrition
- Use multiple metrics together for comprehensive insights

### Common Analysis Patterns
- **Trend Analysis**: Track metrics over time to identify patterns
- **Cohort Analysis**: Compare groups hired in different periods
- **Segmentation**: Break down metrics by department, region, or other dimensions
- **Benchmarking**: Compare against industry standards or historical performance

### Data Limitations
- Attrition calculations assume consistent headcount reporting
- Historical data may have different collection methodologies
- Small population segments may have volatile percentage metrics
- Some metrics may lag due to data processing delays

## Metric Dependencies

### Primary Metrics
- Headcount, Hires, Terminations (direct from source data)

### Derived Metrics
- Attrition Rate (depends on Terminations and Headcount)
- Retention Rate (depends on Attrition Rate)
- Average Tenure (calculated from hire dates)

### Complex Metrics
- Month-over-month changes (require time series calculations)
- Cohort retention (require complex grouping and time analysis)