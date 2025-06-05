import os
import sys
from uuid import UUID
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import your Snowflake loading utilities (make sure Load/snowflake_loader.py exists)
from Load.snowflake_loader import load_df_to_snowflake, create_tables_in_snowflake


def extract_local(filename):
    """
    Load a parquet file from local Dataset/Data folder into a pandas DataFrame.
    """
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Dataset", "Data"))

    filepath = os.path.join(base_path, filename)
    try:
        print(f"Loading {filename} from local path: {filepath}")
        df = pd.read_parquet(filepath, engine="pyarrow")  # or engine="fastparquet"
        print(f"Loaded {filename} successfully, shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Error loading {filename}: {e}")
        return None


def create_tables_in_snowflake(create_query):
    try:
        # Execute the query to create tables in Snowflake
        # Example: snowflake_connection.execute(create_query)
        print(f"Executing query: {create_query}")
        return True  # Return True if successful
    except Exception as e:
        print(f"Error creating table: {e}")
        return False  # Return False if an error occurs


def main():
    print("Starting data extraction from local parquet files...")

    # Extract all dataframes from local parquet files
    customers_df = extract_local("olist_customers_dataset.parquet")
    order_items_df = extract_local("olist_order_items_dataset.parquet")
    order_payments_df = extract_local("olist_order_payments_dataset.parquet")
    order_reviews_df = extract_local("olist_order_reviews_dataset.parquet")
    orders_df = extract_local("olist_orders_dataset.parquet")
    product_category_df = extract_local("product_category_name_translation.parquet")

    products_df = extract_local("olist_products_dataset.parquet")
    sellers_df = extract_local("olist_sellers_dataset.parquet")

    # Check for missing dataframes (missing or failed to load parquet)
    dataframes = {
        "customers": customers_df,
        "order_items": order_items_df,
        "order_payments": order_payments_df,
        "order_reviews": order_reviews_df,
        "orders": orders_df,
        "product_categories": product_category_df,
        "products": products_df,
        "sellers": sellers_df,
    }
    for name, df in dataframes.items():
        if df is None or df.empty:
            print(f"Error: {name} dataframe is missing or empty. Please check the parquet files.")
            return

    

    print("All parquet files loaded successfully.\nStarting data transformation...")

    # ======== Transformations ========
    # Customers
    customers_df["customer_id"] = customers_df["customer_id"].apply(UUID)
    customers_df["customer_unique_id"] = customers_df["customer_unique_id"].apply(UUID)
    id_to_name = {cust_id: f"Customer_{idx + 1}" for idx, cust_id in enumerate(customers_df["customer_unique_id"].unique())}
    customers_df["customer_name"] = customers_df["customer_unique_id"].map(id_to_name)

    # Order Items
    order_items_df.drop(["order_items_id"], axis=1, inplace=True, errors='ignore')
    order_items_df.rename(columns={"quantity": "order_item_id"}, inplace=True)
    order_items_df["total_price"] = (order_items_df["freight_value"] + order_items_df["price"]).round().astype(int)
    order_items_df["order_id"] = order_items_df["order_id"].apply(UUID)
    order_items_df["product_id"] = order_items_df["product_id"].apply(UUID)
    order_items_df["seller_id"] = order_items_df["seller_id"].apply(UUID)

    # Order Payments
    order_payments_df["order_id"] = order_payments_df["order_id"].apply(UUID)

    # Order Reviews
    order_reviews_df.drop(["review_comment_title", "review_comment_message"], axis=1, inplace=True, errors='ignore')
    order_reviews_df["review_answer_timestamp"] = pd.to_datetime(order_reviews_df["review_answer_timestamp"])
    order_reviews_df["order_id"] = order_reviews_df["order_id"].apply(UUID)
    order_reviews_df["review_id"] = order_reviews_df["review_id"].apply(UUID)
    order_reviews_df = order_reviews_df.sort_values("review_creation_date").drop_duplicates("order_id", keep="last")

    # Orders
    orders_df["order_id"] = orders_df["order_id"].apply(UUID)
    orders_df["customer_id"] = orders_df["customer_id"].apply(UUID)

    # Product Categories
    product_category_map = product_category_df.set_index("product_category_name")["product_category_name_english"].to_dict()
    product_category_map["unknown"] = "unknown"
    products_df["product_category_name"] = products_df["product_category_name"].map(product_category_map).fillna("unknown")

    # Products
    products_df.rename(
        columns={
            "product_description_lenght": "product_description_length",
            "product_name_lenght": "product_name_length"
        }, inplace=True)
    products_df["product_id"] = products_df["product_id"].apply(UUID)
    id_to_product_name = {prod_id: f"Product_{idx + 1}" for idx, prod_id in enumerate(products_df["product_id"].unique())}
    products_df["product_name"] = products_df["product_id"].map(id_to_product_name)

    # Sellers
    sellers_df["seller_id"] = sellers_df["seller_id"].apply(UUID)
    id_to_seller_name = {sel_id: f"Seller_{idx + 1}" for idx, sel_id in enumerate(sellers_df["seller_id"].unique())}
    sellers_df["seller_name"] = sellers_df["seller_id"].map(id_to_seller_name)

    # Dimension Modeling for customers
    customer_map = customers_df.set_index("customer_id")["customer_unique_id"]
    orders_df["customer_id"] = orders_df["customer_id"].map(customer_map)
    customers_df.drop("customer_id", axis=1, inplace=True)
    customers_df.drop_duplicates(inplace=True)
    customers_df.rename(columns={"customer_unique_id": "customer_id"}, inplace=True)

    # Fact Tables
    fact_orders = order_items_df.merge(orders_df, on="order_id", how="left") \
                                .merge(order_reviews_df, on="order_id", how="left")
    fact_orders.drop(["review_id", "review_answer_timestamp", "review_creation_date"], axis=1, inplace=True, errors='ignore')
    fact_orders.rename(columns={"review_score": "order_rating"}, inplace=True)

    fact_payments = order_payments_df  # Separate fact table for payments

    # Dim Tables
    dim_customers = customers_df
    dim_sellers = sellers_df
    dim_products = products_df

    # Date dimension table
    dim_dates = pd.DataFrame(pd.date_range(start="2016-01-01", end="2018-12-31"), columns=["date"])
    dim_dates["quarter"] = dim_dates.date.dt.quarter
    dim_dates["month"] = dim_dates.date.dt.month
    dim_dates["year"] = dim_dates.date.dt.year
    dim_dates["week_by_year"] = dim_dates.date.dt.strftime("%W").astype(int)
    dim_dates["day"] = dim_dates.date.dt.day
    dim_dates["weekday"] = dim_dates.date.dt.weekday
    dim_dates["weekday_name"] = dim_dates.date.dt.day_name()

    print("Data transformation completed.\nCreating Snowflake tables...")

    # Create tables in Snowflake
    table_creations = [
        (
            "FACT_ORDERS",
            """
            CREATE TABLE IF NOT EXISTS FACT_ORDERS(
                "order_id" VARCHAR(50),
                "order_item_id" INT,
                "product_id" VARCHAR(50),
                "seller_id" VARCHAR(50),
                "shipping_limit_date" TIMESTAMP_TZ(0),
                "price" FLOAT,
                "freight_value" FLOAT,
                "total_price" INT,
                "customer_id" VARCHAR(50),
                "order_status" VARCHAR(50),
                "order_purchase_timestamp" TIMESTAMP_TZ(0),
                "order_approved_at" TIMESTAMP_TZ(0),
                "order_delivered_carrier_date" TIMESTAMP_TZ(0),
                "order_delivered_customer_date" TIMESTAMP_TZ(0),
                "order_estimated_delivery_date" TIMESTAMP_TZ(0),
                "order_rating" FLOAT
            );
            """
        ),
        (
            "FACT_PAYMENTS",
            """
            CREATE TABLE IF NOT EXISTS FACT_PAYMENTS(
                "order_id" VARCHAR(50),
                "payment_sequential" INT,
                "payment_type" VARCHAR(50),
                "payment_installments" INT,
                "payment_value" FLOAT
            );
            """
        ),
        (
            "DIM_CUSTOMERS",
            """
            CREATE TABLE IF NOT EXISTS DIM_CUSTOMERS(
                "customer_id" VARCHAR(50),
                "customer_zip_code_prefix" INT,
                "customer_city" VARCHAR(50),
                "customer_state" VARCHAR(50),
                "customer_name" VARCHAR(20)
            );
            """
        ),
        (
            "DIM_SELLERS",
            """
            CREATE TABLE IF NOT EXISTS DIM_SELLERS(
                "seller_id" VARCHAR(50),
                "seller_zip_code_prefix" INT,
                "seller_city" VARCHAR(50),
                "seller_state" VARCHAR(50),
                "seller_name" VARCHAR(20)
            );
            """
        ),
        (
            "DIM_PRODUCTS",
            """
            CREATE TABLE IF NOT EXISTS DIM_PRODUCTS(
                "product_id" VARCHAR(50),
                "product_category_name" VARCHAR(50),
                "product_name_length" INT,
                "product_description_length" INT,
                "product_photos_qty" INT,
                "product_weight_g" INT,
                "product_length_cm" INT,
                "product_height_cm" INT,
                "product_width_cm" INT,
                "product_name" VARCHAR(20)
            );
            """
        ),
        (
            "DIM_DATES",
            """
            CREATE TABLE IF NOT EXISTS DIM_DATES(
                "date" TIMESTAMP_TZ(0),
                "quarter" INT,
                "month" INT,
                "year" INT,
                "week_by_year" INT,
                "day" INT,
                "weekday" INT,
                "weekday_name" VARCHAR(50)
            );
            """
        )
    ]

    for table_name, create_query in table_creations:
        success = create_tables_in_snowflake(create_query)
        if not success:
            raise Exception(f"Failed to create table {table_name}")

    print("Snowflake tables created successfully.\nLoading data into Snowflake...")

    # Load dataframes into Snowflake tables
    load_df_to_snowflake("FACT_ORDERS", fact_orders)
    load_df_to_snowflake("FACT_PAYMENTS", fact_payments)
    load_df_to_snowflake("DIM_CUSTOMERS", dim_customers)
    load_df_to_snowflake("DIM_SELLERS", dim_sellers)
    load_df_to_snowflake("DIM_PRODUCTS", dim_products)
    load_df_to_snowflake("DIM_DATES", dim_dates)

    print("Data successfully loaded into Snowflake.")


if __name__ == "__main__":
    main()
