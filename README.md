# E-Commerce Data Pipeline Project

This project is a data pipeline designed to extract, transform, and load (ETL) e-commerce datasets from local parquet files, S3 storage, and load them into Snowflake data warehouse for analytics and reporting.

---

## Project Overview

This pipeline consists of several modular components to handle data movement between local storage, AWS S3, databases, and Snowflake:

- **Extract**: Reads data from local parquet files or S3 buckets.
- **Transform**: Cleans and enriches raw data to create dimension and fact tables.
- **Load**: Loads transformed data into Snowflake tables.
- **Supporting Modules**:
  - `db_loader.py`: Handles database interactions.
  - `s3_loader.py`: Manages upload/download of files to/from AWS S3.
  - `snowflake_loader.py`: Contains functions to create Snowflake tables and load pandas DataFrames.
  - `db_to_s3.py`: Facilitates extraction from a database and loading to S3.
  - `s3_extract.py`: Extracts files from S3 to local storage.

---

## Features

- Modular ETL functions supporting multiple data sources.
- UUID conversions and datetime standardization during transformation.
- Dimension and fact table modeling for efficient analytics.
- Snowflake table creation and batch loading with logging.
- Configurable via environment variables for security and flexibility.

---

## Requirements

- Python 3.8+
- Packages:
  - `pandas`
  - `numpy`
  - `pyarrow` (or `fastparquet` for parquet file reading)
  - `snowflake-connector-python`
  - `python-dotenv`
  - `boto3` (for AWS S3 interaction)
- Access to Snowflake account with necessary credentials.
- AWS credentials configured for S3 access (if using S3 modules).

---

## Setup

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/your-repo-name.git
    cd your-repo-name
    ```

2. Create a `.env` file in the root directory with the following environment variables:

    ```ini
    SNOWFLAKE_USER=your_snowflake_username
    SNOWFLAKE_PASSWORD=your_snowflake_password
    SNOWFLAKE_ACCOUNT=your_snowflake_account_locator_with_region
    SNOWFLAKE_WAREHOUSE=your_warehouse_name
    SNOWFLAKE_DATABASE=your_database_name
    SNOWFLAKE_SCHEMA=your_schema_name

    AWS_ACCESS_KEY_ID=your_aws_access_key
    AWS_SECRET_ACCESS_KEY=your_aws_secret_key
    AWS_S3_BUCKET=your_s3_bucket_name
    ```

3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

---

## Usage

### Run the main pipeline

```bash
python main.py
