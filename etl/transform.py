import pandas as pd

def transform_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw event data.
    """
    # Standardize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Convert timestamp
    ts_col = "event_time" if "event_time" in df.columns else None
    if ts_col:
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
        df = df.dropna(subset=[ts_col])
        df["event_date"] = df[ts_col].dt.date

    # Standardize event_name
    if "event_name" in df.columns:
        df["event_name"] = df["event_name"].str.strip().str.lower()

    # Fill numeric nulls
    for col in df.select_dtypes(include=["int64","float64"]):
        df[col] = df[col].fillna(0)

    # Remove duplicates
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
