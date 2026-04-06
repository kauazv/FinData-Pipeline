import pandas as pd
import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "data/silver")
REQUIRED_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume"]

os.makedirs(SILVER_PATH, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("processing.process_market_data")


def get_latest_file():
    files = [f for f in os.listdir(BRONZE_PATH) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado na camada bronze")
    files.sort()
    return os.path.join(BRONZE_PATH, files[-1])


def _load_legacy_bronze(file_path):
    legacy_df = pd.read_csv(file_path, header=[0, 1])
    legacy_df.columns = [(str(c1).strip().lower(), str(c2).strip()) for c1, c2 in legacy_df.columns]

    date_column = None
    for column in legacy_df.columns:
        if column[0] == "date":
            date_column = column
            break

    if date_column is None:
        raise ValueError("Formato legado inválido: coluna de data não encontrada")

    tickers = sorted(
        {
            c2
            for c1, c2 in legacy_df.columns
            if c2 and "unnamed" not in c2.lower() and c1 in {"open", "high", "low", "close", "volume"}
        }
    )
    if not tickers:
        raise ValueError("Formato legado inválido: nenhum ticker encontrado")

    frames = []
    for ticker in tickers:
        required_ticker_columns = [("open", ticker), ("high", ticker), ("low", ticker), ("close", ticker), ("volume", ticker)]
        if not all(col in legacy_df.columns for col in required_ticker_columns):
            continue

        ticker_df = pd.DataFrame(
            {
                "date": legacy_df[date_column],
                "symbol": ticker,
                "open": legacy_df[("open", ticker)],
                "high": legacy_df[("high", ticker)],
                "low": legacy_df[("low", ticker)],
                "close": legacy_df[("close", ticker)],
                "volume": legacy_df[("volume", ticker)],
            }
        )
        frames.append(ticker_df)

    if not frames:
        raise ValueError("Formato legado inválido: não foi possível normalizar os tickers")

    return pd.concat(frames, ignore_index=True)


def load_bronze(file_path):
    df = pd.read_csv(file_path)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    has_legacy_suffix_columns = any(column.rsplit(".", 1)[-1].isdigit() for column in df.columns if "." in column)
    if set(REQUIRED_COLUMNS).issubset(df.columns) and not has_legacy_suffix_columns:
        return df[REQUIRED_COLUMNS]
    return _load_legacy_bronze(file_path)


def process_data():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info("Iniciando processamento silver | run_id=%s", run_id)

    file_path = get_latest_file()

    df = load_bronze(file_path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    numeric_columns = ["open", "high", "low", "close", "volume"]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # tratar valores nulos apenas nas colunas-chave do contrato
    df = df.dropna(subset=REQUIRED_COLUMNS)

    # remover duplicados após limpeza para preservar registros válidos
    df = df.drop_duplicates(subset=["date", "symbol"])

    df["date"] = df["date"].dt.date
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    output_file = os.path.join(SILVER_PATH, os.path.basename(file_path))

    df.to_csv(output_file, index=False)

    logger.info("Processamento finalizado | run_id=%s | linhas=%d | arquivo=%s", run_id, len(df), output_file)


if __name__ == "__main__":
    process_data()
