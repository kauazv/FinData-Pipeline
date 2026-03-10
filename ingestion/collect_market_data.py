import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

URL = "https://api.coingecko.com/api/v3/coins/markets"

PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1
}


def fetch_market_data():
    """Coleta dados da API"""
    response = requests.get(URL, params=PARAMS)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data)

    return df


def save_raw_data(df):
    """Salva dados na camada bronze"""
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    path = Path("data/bronze")
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"market_data_{timestamp}.csv"

    df.to_csv(file_path, index=False)

    print(f"Dados salvos em {file_path}")


def main():

    df = fetch_market_data()

    save_raw_data(df)


if __name__ == "__main__":
    main()