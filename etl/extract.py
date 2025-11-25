import pandas as pd
from utils.file_loader import load_csv

def extract_raw_data(s3_path: str, local_path: str) -> pd.DataFrame:
    df = load_csv(s3_path, local_path)

    # Clean columns and remove duplicates / empty rows
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.drop_duplicates().dropna(how="all")

    return df

if __name__ == "__main__":
    S3_PATH = "s3://your-bucket-name/raw/events.csv"
    LOCAL_PATH = "./data/raw/events.csv"

    df = extract_raw_data(S3_PATH, LOCAL_PATH)
    print(df.head())
    print(f"Rows extracted: {len(df)}")
