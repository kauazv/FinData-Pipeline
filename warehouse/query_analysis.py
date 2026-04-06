import duckdb
import os


def get_latest_db_path():
    files = [f for f in os.listdir("warehouse") if f.endswith(".duckdb") and f.startswith("market_data")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo DuckDB encontrado em warehouse/")
    files.sort()
    return os.path.join("warehouse", files[-1])

db_path = get_latest_db_path()

with duckdb.connect(db_path, read_only=True) as con:
    result = con.execute("""
    SELECT
        symbol,
        AVG(return) AS avg_return,
        AVG(volatility_5) AS avg_volatility
    FROM market_features
    GROUP BY symbol
    ORDER BY avg_return DESC
    """).fetchdf()

print(f"Consultando base: {db_path}")
print(result)
