import os
import logging
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from Load.s3_loader import upload_df_to_s3

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB", "Olist_Ecommerce")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
BUCKET_NAME = os.getenv("BUCKET_NAME")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:"
    f"{POSTGRES_PASSWORD}@{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/{POSTGRES_DB}"
)
engine = create_engine(DATABASE_URL)


def extract_table(table_name: str) -> pd.DataFrame:
    """
    Extract data from the specified table in the Postgres database.

    Args:
        table_name (str): Name of the table to extract.

    Returns:
        pd.DataFrame: Extracted data as a DataFrame.
    """
    logging.info(f"Extracting data from table: {table_name}")
    df = pd.read_sql_table(table_name, engine)
    return df


def main():
    """
    Main function to extract data from tables and upload them to S3 as Parquet files.
    """
    table_names = (
        "customers",
        "geolocations",
        "product_categories",
        "products",
        "orders",
        "order_items",
        "order_reviews",
        "sellers",
        "order_payments",
    )

    for table_name in table_names:
        try:
            df = extract_table(table_name)
            success = upload_df_to_s3(df, bucket=BUCKET_NAME, key=f"Data/{table_name}.parquet")
            if not success:
                raise Exception(f"Failed to upload {table_name} to S3.")
            logging.info(f"Successfully uploaded {table_name} to S3.")
        except Exception as e:
            logging.error(f"Error processing table {table_name}: {e}")
            # Optional: Decide whether to continue or exit
            # For now, continue with other tables
            continue


if __name__ == "__main__":
    main()
