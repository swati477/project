import pandas as pd
import sqlite3
from sqlalchemy import create_engine

def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str):
    """
    Load transformed data into a local SQLite database.

    Args:
        df (DataFrame): Cleaned data to load
        db_path (str): Path to SQLite file (example: './warehouse.db')
        table_name (str): Table name inside SQLite
    """

    # Create a connection using SQLAlchemy
    engine = create_engine(f"sqlite:///{db_path}")

    # Load data into SQLite
    df.to_sql(table_name, engine, if_exists="replace", index=False)

    print(f"✔ Loaded {len(df)} rows into SQLite → {db_path} → table: {table_name}")


if __name__ == "__main__":
    from extract import extract_raw_data
    from transform import transform_raw_data

    # 1. Extract raw data
    S3_PATH = "s3://your-bucket/raw/events.csv"
    LOCAL_PATH = "./data/raw/events.csv"
    raw = extract_raw_data(S3_PATH, LOCAL_PATH)

    # 2. Transform
    clean = transform_raw_data(raw)

    # 3. Load into SQLite
    DB_PATH = "./warehouse.db"    # file will auto-create
    TABLE = "fact_events"

    load_to_sqlite(clean, DB_PATH, TABLE)
