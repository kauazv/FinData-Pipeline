import duckdb
import pandas as pd
import os
from datetime import datetime, timezone
import logging
from dotenv import load_dotenv

load_dotenv()

GOLD_PATH = os.getenv("GOLD_PATH", "data/gold")
DB_PATH = os.getenv("DUCKDB_PATH", "warehouse/market_data.duckdb")
TABLE_NAME = os.getenv("DUCKDB_TABLE", "market_features")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("warehouse.load_to_duckdb")


def get_latest_file():

    files = [f for f in os.listdir(GOLD_PATH) if f.endswith(".csv")]
    files.sort()

    return os.path.join(GOLD_PATH, files[-1])


def load_data():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info("Iniciando carga no DuckDB | run_id=%s", run_id)

    file_path = get_latest_file()

    df = pd.read_csv(file_path)
    if df.empty:
        raise ValueError("Camada gold está vazia. Interrompendo carga no DuckDB.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date", "symbol"])
    df = df.drop_duplicates(subset=["date", "symbol"])

    target_db_path = DB_PATH
    try:
        con = duckdb.connect(target_db_path)
    except duckdb.IOException as exc:
        fallback_name = f"market_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
        target_db_path = os.path.join("warehouse", fallback_name)
        logger.warning("Falha ao abrir DB padrão (%s). Usando fallback: %s", exc, target_db_path)
        con = duckdb.connect(target_db_path)

    con.register("incoming_df", df)

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} AS
        SELECT * FROM incoming_df WHERE 1=0
    """)

    before_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    con.execute(f"""
        INSERT INTO {TABLE_NAME}
        SELECT i.*
        FROM incoming_df i
        LEFT JOIN {TABLE_NAME} m
          ON m.date = i.date
         AND m.symbol = i.symbol
        WHERE m.date IS NULL
    """)
    after_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]

    con.close()

    logger.info(
        "Carga finalizada | run_id=%s | db=%s | linhas_entrada=%d | inseridas=%d | total_tabela=%d",
        run_id,
        target_db_path,
        len(df),
        after_count - before_count,
        after_count
    )


if __name__ == "__main__":
    load_data()
