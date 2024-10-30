
# Customer Engagement Analytics ETL Project

## Overview
This project analyzes customer engagement data by extracting, transforming, and loading (ETL) streaming log data from AWS S3. The objective is to generate insights on customer engagement levels, content preferences, and usage patterns based on 30 days of log data in watch history from April 2022.

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

### Step 3: Load
The transformed dataset is loaded into a data warehouse MYSQL by JDBC as well as save into a local csv file for further analysis.

## Analytics Overview

The transformed data enables the following analytics:

### 1. Customer Segmentation
Classify customers based on viewing habits:
- Activity levels (`Active` column).
- Content preferences (`MostWatch` column).

### 2. Engagement Analysis
Identify popular content categories and engagement distribution:
- Summarize total viewing time for each content type.
- Calculate distribution metrics across all customers.

### 3. Loyalty and Retention Insights
Analyze customer loyalty and potential churn indicators:
- Identify customers at risk of churn.
- Measure the impact of `MostWatch` content type on loyalty.

### 4. Trend Analysis
Identify patterns in content consumption:
- Monthly and cross-category consumption trends.
- Correlations between different content types.

### 5. Descriptive Statistics
Generate key metrics:
- Summary statistics (mean, median, std) for viewing times.
- Count of customers per `MostWatch` category.

### 6. Visualization
Suggested visualizations include:
- **Content Popularity Bar Chart**: Total engagement per content type.
- **Customer Segmentation Scatter Plot**: Display customer clusters by engagement and content preference.
- **Correlation Matrix**: Visualize correlations between content types.

## Getting Started

1. **ETL Script**: Implement the ETL process by running the script in the `etl/` directory.
2. **Data Validation**: Ensure that all required fields are correctly transformed and populated.
3. **Analytics**: Load the transformed data into the designated analytics environment and apply the above analyses.
