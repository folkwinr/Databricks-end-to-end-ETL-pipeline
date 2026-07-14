# End-to-End ETL Pipeline with Databricks and AWS S3 

This project is an end-to-end ETL pipeline I built to practice data ingestion, transformation, and pipeline automation in Databricks.

The main idea is simple: transaction files are added to an AWS S3 bucket, ingested into Databricks, cleaned and validated, and finally transformed into a daily transaction summary.

I used the Medallion Architecture to organize the data into Bronze, Silver, and Gold layers.

##  How the Pipeline Works

The overall data flow looks like this:

```text
AWS S3
   ↓
Data Ingestion
   ↓
Streaming Table
   ↓
Table Update Trigger
   ↓
Databricks Job
   ↓
ETL Pipeline
   ↓
Bronze → Silver → Gold
```

When a new transaction file is added to the S3 bucket, the ingestion process loads the new data into the source streaming table.

Once the table is updated, a table update trigger starts the Databricks Job. The Job then runs the ETL pipeline and processes the data through the Bronze, Silver, and Gold layers.

The goal was to make the entire flow run automatically without manually starting the ETL pipeline each time new data arrives.

##  Bronze Layer

The Bronze layer reads transaction data from the source streaming table.

At this stage, I keep the data in its raw form without applying cleaning or business rules.

The Bronze table is created as:

`bronze_transactions`

The source data is read using Spark Structured Streaming:

```python
spark.readStream.table("dataengineering.endtoend.transactions")
```

Keeping the raw data separate makes it easier to trace the original source data before transformations are applied.

##  Silver Layer

The Silver layer is responsible for cleaning and validating the transaction data.

I used PySpark transformations and pipeline expectations to apply several data quality rules.

The following steps are applied:

- Filter records with missing transaction IDs
- Filter records with missing transaction dates
- Filter records with missing customer IDs
- Remove duplicate transactions based on `transaction_id`
- Remove the `_rescued_data` column
- Drop records where `quantity` is not positive
- Drop records with negative `unit_price`
- Drop records with negative `total_amount`

The cleaned data is stored in:

`silver_transactions_clean`

Databricks pipeline expectations are also used to validate quantity, unit price, and total amount values before the records continue through the pipeline.

##  Gold Layer

The Gold layer creates a daily summary from the cleaned transaction data.

The Silver data is grouped by transaction date and used to calculate:

- Total number of transactions
- Total revenue
- Average transaction amount
- Total quantity sold
- Number of unique customers
- Categories sold
- Store locations
- Payment methods

The final materialized view is:

`gold_daily_transactions`

This layer represents the analytics-ready output of the pipeline and could be used for reporting or dashboarding.

## ⚙️ Automation

One of the main things I wanted to practice in this project was pipeline automation.

Instead of manually running the ETL process, I configured the workflow so that a table update can trigger the Databricks Job.

The flow is:

1. A new transaction file is added to AWS S3.
2. The ingestion process loads the new data.
3. The source streaming table is updated.
4. The table update trigger starts the Databricks Job.
5. The Job runs the ETL pipeline.
6. Bronze, Silver, and Gold layers are updated.

I tested the flow by adding another transaction file to the S3 bucket and verifying that the new records were ingested and processed through the pipeline.

## 📁 Project Structure

```text
src/
├── bronze_transactions.py
├── silver_transactions_clean.py
└── gold_daily_transactions.py
```

- `bronze_transactions.py` reads the raw transaction data.
- `silver_transactions_clean.py` cleans and validates the data.
- `gold_daily_transactions.py` creates the daily transaction metrics.

##  Technologies Used

- Databricks
- AWS S3
- PySpark
- Lakeflow Declarative Pipelines
- Databricks Jobs
- Streaming Tables
- Medallion Architecture

##  What I Practiced

Through this project, I practiced building a complete data flow from ingestion to an analytics-ready dataset.

The main topics I focused on were:

- Data ingestion from AWS S3
- Bronze, Silver, and Gold data layers
- PySpark transformations
- Data quality expectations
- Streaming data ingestion
- ETL pipeline development
- Job orchestration
- Trigger-based pipeline automation

This project helped me better understand how the different parts of a data engineering pipeline work together in Databricks.
