import yfinance as yf
import pandas as pd
from datetime import datetime
import os

BRONZE_PATH = "data/bronze"

os.makedirs(BRONZE_PATH, exist_ok=True)

TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "NVDA"
]


def collect_data():

    all_data = []

    for ticker in TICKERS:

        df = yf.download(
            ticker,
            period="30d",
            interval="1d"
        )

        df["symbol"] = ticker
        df.reset_index(inplace=True)

        all_data.append(df)

    final_df = pd.concat(all_data)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    file_path = f"{BRONZE_PATH}/market_data_{timestamp}.csv"

    final_df.to_csv(file_path, index=False)

    print(f"Dados coletados e salvos em {file_path}")


if __name__ == "__main__":
    collect_data()