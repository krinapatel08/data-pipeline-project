import os
from pathlib import Path
import logging

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (UUID, DateTime, Float, Integer, String, Text,
                        create_engine)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_PATH = BASE_DIR / "Dataset/Raw-Dataset/E-Commerce/"
load_dotenv(BASE_DIR / ".env")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB", "Olist_Ecommerce")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

# Check environment variables
CREDENTIALS = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_PORT", "POSTGRES_HOST")
if not all(os.getenv(cred) for cred in CREDENTIALS):
    raise ValueError(f"Please set environment variables {CREDENTIALS}")

# sqlalchemy variables
DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:"
    f"{POSTGRES_PASSWORD}@{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/{POSTGRES_DB}"
)
engine = create_engine(DATABASE_URL)


def load_table_from_csv(table_name: str, csv_path: Path, dtype: dict = None, 
                        if_exists: str = "replace", chunksize: int = None, 
                        additional_steps=None):
    """
    Generic function to load a CSV into a PostgreSQL table using pandas.to_sql.
    Args:
        table_name (str): Destination table name.
        csv_path (Path): Path to the CSV file.
        dtype (dict): Optional SQLAlchemy types for columns.
        if_exists (str): 'replace', 'append', or 'fail' for table existence.
        chunksize (int): Number of rows per batch insert.
        additional_steps (callable): Optional function to process dataframe before load.
    """
    logging.info(f"Loading {table_name} from {csv_path}")
    df = pd.read_csv(csv_path)
    if additional_steps:
        df = additional_steps(df)
    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False,
        dtype=dtype,
        method="multi" if chunksize else None,
        chunksize=chunksize,
    )
    logging.info(f"Loaded {len(df)} records into {table_name}")


def preprocess_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns and calculate total_price for order_items"""
    df.rename(columns={"order_item_id": "quantity"}, inplace=True)
    df["order_items_id"] = df.index
    df["total_price"] = df["quantity"] * df["price"] + df["freight_value"]
    return df


def main():
    try:
        load_table_from_csv(
            "customers",
            DATASET_PATH / "olist_customers_dataset.csv",
            dtype={
                "customer_id": UUID,
                "customer_unique_id": UUID,
                "customer_zip_code_prefix": Integer,
                "customer_city": String(50),
                "customer_state": String(5),
            }
        )

        load_table_from_csv(
            "geolocations",
            DATASET_PATH / "olist_geolocation_dataset.csv",
            dtype={
                "geolocation_zip_code_prefix": Integer,
                "geolocation_lat": Float,
                "geolocation_lon": Float,
                "geolocation_city": String(50),
                "geolocation_state": String(5),
            },
            chunksize=10000
        )

        load_table_from_csv(
            "order_items",
            DATASET_PATH / "olist_order_items_dataset.csv",
            dtype={
                "order_items_id": Integer,
                "order_id": UUID,
                "quantity": Integer,
                "product_id": UUID,
                "seller_id": UUID,
                "shipping_limit_date": DateTime,
                "price": Float,
                "freight_value": Float,
                "total_price": Float,
            },
            chunksize=10000,
            additional_steps=preprocess_order_items
        )

        load_table_from_csv(
            "order_payments",
            DATASET_PATH / "olist_order_payments_dataset.csv",
            dtype={
                "order_id": UUID,
                "payment_sequential": Integer,
                "payment_type": String(50),
                "payment_installments": Integer,
                "payment_value": Float,
            },
            chunksize=10000
        )

        load_table_from_csv(
            "order_reviews",
            DATASET_PATH / "olist_order_reviews_dataset.csv",
            dtype={
                "review_id": UUID,
                "order_id": UUID,
                "review_comment_title": String(50),
                "review_comment_message": Text,
                "review_creation_date": DateTime,
                "review_answer_time": DateTime,
            },
            chunksize=10000
        )

        load_table_from_csv(
            "orders",
            DATASET_PATH / "olist_orders_dataset.csv",
            dtype={
                "order_id": UUID,
                "customer_id": UUID,
                "order_status": String(50),
                "order_purchase_timestamp": DateTime,
                "order_approved_at": DateTime,
                "order_delivered_carrier_date": DateTime,
                "order_delivered_customer_date": DateTime,
                "order_estimated_delivery_date": DateTime,
            },
            chunksize=10000
        )

        load_table_from_csv(
            "sellers",
            DATASET_PATH / "olist_sellers_dataset.csv",
            dtype={
                "seller_id": UUID,
                "seller_zip_code_prefix": Integer,
                "seller_state": String(5),
                "seller_city": String(50),
            }
        )

        load_table_from_csv(
            "products",
            DATASET_PATH / "olist_products_dataset.csv",
            dtype={
                "product_id": UUID,
                "product_category_name": String(100),
                "product_name_lenght": Integer,
                "product_description_lenght": Integer,
                "product_photos_qty": Integer,
                "product_weight_g": Integer,
                "product_length_cm": Integer,
                "product_height_cm": Integer,
                "product_width_cm": Integer,
            }
        )

        load_table_from_csv(
            "product_categories",
            DATASET_PATH / "product_category_name_translation.csv",
            dtype={
                "product_category_name": String(100),
                "product_category_name_english": String(100),
            }
        )
    except Exception as e:
        logging.error(f"Error occurred during ETL process: {e}")
        raise


if __name__ == "__main__":
    main()
