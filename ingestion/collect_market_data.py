import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import os
import logging
from dotenv import load_dotenv

load_dotenv()

SYMBOLS = [s.strip().upper() for s in os.getenv("TICKERS", "AAPL,MSFT,NVDA,GOOGL,AMZN").split(",") if s.strip()]
PERIOD = os.getenv("YF_PERIOD", "6mo")
INTERVAL = os.getenv("YF_INTERVAL", "1d")
OUTPUT_PATH = os.getenv("BRONZE_PATH", "data/bronze")
CACHE_PATH = os.getenv("YF_CACHE_PATH", "data/cache/yfinance")
REQUIRED_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume"]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("ingestion.collect_market_data")


def collect_data():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info("Iniciando coleta | run_id=%s | tickers=%s | period=%s | interval=%s", run_id, SYMBOLS, PERIOD, INTERVAL)

    os.makedirs(CACHE_PATH, exist_ok=True)
    yf.set_tz_cache_location(CACHE_PATH)

    all_data = []

    for symbol in SYMBOLS:

        df = yf.download(
            symbol,
            period=PERIOD,
            interval=INTERVAL,
            auto_adjust=False,
            progress=False,
            multi_level_index=False
        )

        if df.empty:
            logger.warning("Sem dados para %s | run_id=%s", symbol, run_id)
            continue

        df["symbol"] = symbol
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

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    file_path = f"{OUTPUT_PATH}/market_data_{timestamp}.csv"

    final_df.to_csv(file_path, index=False)

    logger.info("Coleta finalizada | run_id=%s | linhas=%d | arquivo=%s", run_id, len(final_df), file_path)


if __name__ == "__main__":
    collect_data()
