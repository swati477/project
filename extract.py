import pandas as pd
from utils.file_loader import load_csv

def extract_raw_data(s3_path: str, local_path: str) -> pd.DataFrame:
    """
    Extract raw CSV data using S3 → Local fallback logic.

    Args:
        s3_path (str): S3 file path (s3://bucket/file.csv)
        local_path (str): Local file path fallback (./data/file.csv)

    Returns:
        pd.DataFrame: Raw extracted data
    """

    # Load the CSV (this will automatically try S3, then local)
    df = load_csv(s3_path, local_path)

    # Basic clean-up
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Remove rows with all null values
    df = df.dropna(how="all")

    return df


if __name__ == "__main__":
    # Test run
    S3_PATH = "s3://your-bucket-name/raw/events.csv"
    LOCAL_PATH = "./data/raw/events.csv"

    data = extract_raw_data(S3_PATH, LOCAL_PATH)
    print(data.head())
    print(f"Rows extracted: {len(data)}")
