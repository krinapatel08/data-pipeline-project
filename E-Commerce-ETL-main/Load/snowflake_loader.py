import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
from dotenv import load_dotenv
import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Add a helper to verify all environment vars are present
def check_env_vars() -> bool:
    required_vars = {
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
        "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
        "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
    }
    missing = [k for k, v in required_vars.items() if not v]
    if missing:
        logging.error(f"Missing Snowflake environment variables: {missing}")
        return False
    logging.info(f"All required Snowflake env vars are present.")
    return True


def create_tables_in_snowflake(query: str) -> bool:
    """
    Create tables in Snowflake by executing the provided SQL query.

    Returns True if successful, False otherwise.
    """
    if not check_env_vars():
        logging.error("Cannot create tables: environment variables missing.")
        return False

    if not query or not isinstance(query, str) or query.strip() == "":
        logging.error("Invalid or empty SQL query provided for table creation.")
        return False

    logging.info("Connecting to Snowflake to create tables...")
    try:
        with snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            schema=SNOWFLAKE_SCHEMA,
            database=SNOWFLAKE_DATABASE,
        ) as ctx:
            with ctx.cursor() as cursor:
                logging.info(f"Executing query:\n{query}")
                cursor.execute(query)
                logging.info("Table creation query executed successfully.")
                return True
    except snowflake.connector.errors.ProgrammingError as e:
        logging.error(f"Snowflake ProgrammingError: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    return False


def load_df_to_snowflake(table: str, df: pd.DataFrame) -> bool:
    """
    Upload a pandas DataFrame to a Snowflake table using write_pandas.

    Converts UUID-like columns to string and localizes datetime columns to UTC.

    Returns True if upload successful, False otherwise.
    """
    if not check_env_vars():
        logging.error("Cannot upload data: environment variables missing.")
        return False

    if df.empty:
        logging.error("DataFrame is empty. Aborting upload.")
        return False

    if not table or not isinstance(table, str):
        logging.error("Invalid table name provided.")
        return False

    logging.info(f"Starting upload of DataFrame to Snowflake table '{table}'...")

    # Convert UUID-like columns to string
    for column in df.columns:
        if column.endswith("_id") and df[column].dtype == "object":
            df[column] = df[column].astype(str)

    # Localize datetime columns to UTC (if naive)
    datetime_cols = df.select_dtypes(include=["datetime64[ns]"]).columns
    for col in datetime_cols:
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")

    try:
        with snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            schema=SNOWFLAKE_SCHEMA,
            database=SNOWFLAKE_DATABASE,
        ) as ctx:
            success, nchunks, nrows, _ = write_pandas(df=df, table_name=table, conn=ctx, use_logical_type=True)
            if success:
                logging.info(f"Uploaded {nrows} rows in {nchunks} chunks to Snowflake table '{table}'.")
                return True
            else:
                logging.error(f"write_pandas returned failure for table '{table}'.")
    except snowflake.connector.errors.ProgrammingError as e:
        logging.error(f"Snowflake ProgrammingError during upload: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during upload: {e}")

    return False
