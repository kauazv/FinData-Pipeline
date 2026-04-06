import pandas as pd
import os

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"

os.makedirs(GOLD_PATH, exist_ok=True)


def get_latest_file():

    files = [f for f in os.listdir(SILVER_PATH) if f.endswith(".csv")]
    files.sort()

    return os.path.join(SILVER_PATH, files[-1])


def create_gold():

    file_path = get_latest_file()

    df = pd.read_csv(file_path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "symbol", "close"])
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    df["return"] = df.groupby("symbol")["close"].pct_change()

    df["ma_5"] = df.groupby("symbol")["close"].transform(lambda x: x.rolling(5).mean())

    df["volatility_5"] = df.groupby("symbol")["return"].transform(lambda x: x.rolling(5).std())

    output = file_path.replace("silver", "gold")

    df.to_csv(output, index=False)

    print(f"Dataset GOLD salvo em: {output}")


if __name__ == "__main__":
    create_gold()
