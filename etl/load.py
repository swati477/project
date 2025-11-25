import pandas as pd
from sqlalchemy import create_engine

def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str):
    """
    Load DataFrame into SQLite database.
    """
    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"✔ Loaded {len(df)} rows into SQLite → {db_path} → table: {table_name}")


if __name__ == "__main__":
    from extract import extract_raw_data
    from transform import transform_raw_data

    # Paths
    S3_PATH = "s3://your-bucket/raw/events.csv"
    LOCAL_PATH = "./data/raw/events.csv"
    DB_PATH = "./warehouse.db"
    TABLE = "fact_events"

    # Run ETL
    raw = extract_raw_data(S3_PATH, LOCAL_PATH)
    clean = transform_raw_data(raw)
    load_to_sqlite(clean, DB_PATH, TABLE)
