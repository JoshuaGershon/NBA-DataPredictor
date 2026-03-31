import pandas as pd


def load_historical_data(csv_path):
    df = pd.read_csv(csv_path)

    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace(r"[^\w]", "", regex=True)
    )

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.drop_duplicates()

    if "date" in df.columns:
        df = df.sort_values("date")

    return df.reset_index(drop=True)
