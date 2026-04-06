import duckdb
import pandas as pd
import os
from datetime import datetime

GOLD_PATH = "data/gold"
DB_PATH = "warehouse/market_data.duckdb"


def get_latest_file():

    files = [f for f in os.listdir(GOLD_PATH) if f.endswith(".csv")]
    files.sort()

    return os.path.join(GOLD_PATH, files[-1])


def load_data():

    file_path = get_latest_file()

    df = pd.read_csv(file_path)
    if df.empty:
        raise ValueError("Camada gold está vazia. Interrompendo carga no DuckDB.")

    target_db_path = DB_PATH
    try:
        con = duckdb.connect(target_db_path)
    except duckdb.IOException as exc:
        fallback_name = f"market_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
        target_db_path = os.path.join("warehouse", fallback_name)
        print(f"Aviso: falha ao abrir DB padrão ({exc}). Usando fallback: {target_db_path}")
        con = duckdb.connect(target_db_path)

    # registrar dataframe
    con.register("gold_df", df)

    # recriar tabela
    con.execute("DROP TABLE IF EXISTS market_features")

    con.execute("""
        CREATE TABLE market_features AS
        SELECT * FROM gold_df
    """)

    con.close()

    print(f"Dados carregados no Data Warehouse (DuckDB): {target_db_path}")


if __name__ == "__main__":
    load_data()
