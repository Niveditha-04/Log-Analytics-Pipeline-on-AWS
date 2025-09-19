This project sets up a simple log analytics pipeline using **AWS S3, Glue, Athena, and Spark**.  
It takes raw web logs, cleans and parses them, runs data quality checks, and generates error-rate aggregations.  
The processed data can be queried with Athena and visualized in Power BI.

## How it works
1. Raw logs are uploaded to S3 
2. A Glue ETL job parses logs to outputs clean data and DQ metrics
3. Another Glue job aggregates logs into 5-minute buckets with error rates and anomaly scores 
4. Athena tables are created for easy SQL queries.

## Notes
- Replace bucket/region values with your own.
