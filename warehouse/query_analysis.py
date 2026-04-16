import duckdb
import os
from pathlib import Path
import logging
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = os.getenv("DUCKDB_TABLE", "market_features")
DB_PATH = os.getenv("DUCKDB_PATH", "warehouse/market_data.duckdb")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("warehouse.query_analysis")


def get_latest_db_path():
    candidates = []

    configured_db = Path(DB_PATH)
    if configured_db.exists():
        candidates.append(configured_db)

    warehouse_dir = Path("warehouse")
    candidates.extend(warehouse_dir.glob("market_data_*.duckdb"))

    if not candidates:
        raise FileNotFoundError("Nenhum arquivo DuckDB encontrado em warehouse/")

    latest_db = max(candidates, key=lambda p: p.stat().st_mtime)
    return str(latest_db)

db_path = get_latest_db_path()

with duckdb.connect(db_path, read_only=True) as con:
    result = con.execute(f"""
    SELECT
        symbol,
        AVG(return) AS avg_return,
        AVG(volatility_5) AS avg_volatility
    FROM {TABLE_NAME}
    GROUP BY symbol
    ORDER BY avg_return DESC
    """).fetchdf()

logger.info("Consultando base: %s", db_path)
print(result)
