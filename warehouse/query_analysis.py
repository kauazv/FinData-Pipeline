import duckdb
import os
import logging
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = os.getenv("DUCKDB_TABLE", "market_features")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("warehouse.query_analysis")

def get_latest_db_path():
    files = [f for f in os.listdir("warehouse") if f.endswith(".duckdb") and f.startswith("market_data")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo DuckDB encontrado em warehouse/")
    files.sort()
    return os.path.join("warehouse", files[-1])

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
