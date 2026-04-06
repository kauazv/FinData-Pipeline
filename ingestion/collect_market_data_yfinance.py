import yfinance as yf
import pandas as pd
from datetime import datetime
import os

BRONZE_PATH = "data/bronze"
CACHE_PATH = "data/cache/yfinance"
REQUIRED_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume"]

os.makedirs(BRONZE_PATH, exist_ok=True)

TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "NVDA"
]


def collect_data():

    os.makedirs(CACHE_PATH, exist_ok=True)
    yf.set_tz_cache_location(CACHE_PATH)

    all_data = []

    for ticker in TICKERS:

        df = yf.download(
            ticker,
            period="30d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            multi_level_index=False
        )

        if df.empty:
            print(f"Aviso: sem dados para {ticker}")
            continue

        df["symbol"] = ticker
        df.reset_index(inplace=True)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        df = df[REQUIRED_COLUMNS]

        all_data.append(df)

    if not all_data:
        raise RuntimeError("Nenhum dado foi coletado da API")

    final_df = pd.concat(all_data, ignore_index=True)
    final_df["date"] = pd.to_datetime(final_df["date"], errors="coerce")
    final_df = final_df.dropna(subset=REQUIRED_COLUMNS)
    final_df = final_df.drop_duplicates(subset=["date", "symbol"])
    final_df = final_df.sort_values(["symbol", "date"]).reset_index(drop=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    file_path = f"{BRONZE_PATH}/market_data_{timestamp}.csv"

    final_df.to_csv(file_path, index=False)

    print(f"Dados coletados e salvos em {file_path}")


if __name__ == "__main__":
    collect_data()
