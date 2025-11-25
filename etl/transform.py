import pandas as pd

def transform_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw event data.

    Steps:
    1. Standardize column names
    2. Convert timestamp to datetime
    3. Remove invalid events
    4. Create feature columns
    5. Handle nulls
    """

    # 1. Ensure lower_snake_case (safe step)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # 2. Convert timestamp column
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    # Remove rows with invalid date
    df = df.dropna(subset=["event_time"])

    # 3. Standardize event name
    if "event_name" in df.columns:
        df["event_name"] = df["event_name"].str.strip().str.lower()

    # 4. Create feature: event_date
    if "event_time" in df.columns:
        df["event_date"] = df["event_time"].dt.date

    # 5. Fill nulls for metrics
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
    for col in numeric_cols:
        df[col] = df[col].fillna(0)

    # 6. Remove duplicates
    df = df.drop_duplicates()

    return df


if __name__ == "__main__":
    from extract import extract_raw_data

    S3_PATH = "s3://your-bucket-name/raw/events.csv"
    LOCAL_PATH = "./data/raw/events.csv"

    raw = extract_raw_data(S3_PATH, LOCAL_PATH)
    clean = transform_raw_data(raw)

    print(clean.head())
    print(f"Rows after transformation: {len(clean)}")
