
**Customer Engagement Analytics ETL Project**

###  Overview
This project involves extracting, transforming, and loading (ETL) streaming log data from AWS S3 into a data warehouse. Additionally, the `insert_summary_statistics` stored procedure is developed to automate daily insertion of summarized customer engagement data into the `Summary_Daily_Statistics` table within a data mart tailored for the marketing department. The main objectives are to analyze customer engagement, content preferences, and usage patterns based on 30 days of log data from April 2022, providing actionable insights for marketing strategies. 

Let me know if this name or overview needs further refinement!

## Initial Log Data Schema
The raw data from S3 follows this schema:
```plaintext
root
 |-- _id: string (nullable = true)
 |-- _index: string (nullable = true)
 |-- _score: long (nullable = true)
 |-- _source: struct (nullable = true)
 |    |-- AppName: string (nullable = true)
 |    |-- Contract: string (nullable = true)
 |    |-- Mac: string (nullable = true)
 |    |-- TotalDuration: long (nullable = true)
 |-- _type: string (nullable = true)
```

### Field Definitions
- `_id`: Unique identifier for each log entry.
- `_index`: Index information for the log (for Elasticsearch-style logs).
- `_score`: Score associated with the log, typically used for ranking.
- `_source`: Nested object containing:
  - `AppName`: Application name or content type viewed.
  - `Contract`: Customer contract ID.
  - `Mac`: MAC address of the device.
  - `TotalDuration`: Total duration the content was viewed, in seconds.
- `_type`: Type of the log record (for Elasticsearch-style logs).

## ETL Process

### Step 1: Extract
Using AWS SDK for Python (Boto3) or another S3 client, we retrieve log files from the designated S3 bucket. This process loads 30 days of log data from April 2022 for processing.

### Step 2: Transform
The transformation step processes each log entry to create a summary dataset with the following fields:
  - **Contract**: Extracted directly from the log `_source.Contract`.
  - **Total_Giai_Tri**: Aggregated `TotalDuration` for “Giai Tri” content.
  - **Total_Phim_Truyen**: Aggregated `TotalDuration` for “Phim Truyen” content.
  - **Total_The_Thao**: Aggregated `TotalDuration` for “The Thao” content.
  - **Total_Thieu_Nhi**: Aggregated `TotalDuration` for “Thieu Nhi” content.
  - **Total_Truyen_Hinh**: Aggregated `TotalDuration` for “Truyen Hinh” content.
  - **MostWatch**: Category with the highest `TotalDuration` for each customer.
  - **Active**: Customer engagement level calculated based on total viewing duration.
  - **Taste**: Customer taste in watching
### Step 3: Load
The transformed dataset is loaded into a data warehouse MYSQL by JDBC as well as save into a local csv file for further analysis.

## 
