from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from warehouse import load_to_duckdb as wd


def _write_gold_csv(path: Path, df: pd.DataFrame) -> None:
    df.to_csv(path, index=False)


def test_load_data_is_incremental_by_date_symbol(layer_dirs, sample_gold_df):
    wd.GOLD_PATH = str(layer_dirs["gold"])
    wd.DB_PATH = str(layer_dirs["warehouse"] / "test_incremental.duckdb")
    wd.TABLE_NAME = "market_features"

    gold_file = layer_dirs["gold"] / "market_data_20260401_000000.csv"
    _write_gold_csv(gold_file, sample_gold_df)

    wd.load_data()
    wd.load_data()

    with duckdb.connect(wd.DB_PATH, read_only=True) as con:
        count = con.execute(f"SELECT COUNT(*) FROM {wd.TABLE_NAME}").fetchone()[0]

    expected = sample_gold_df.drop_duplicates(subset=["date", "symbol"]).shape[0]
    assert count == expected


def test_load_data_recreates_incompatible_schema(layer_dirs, sample_gold_df):
    wd.GOLD_PATH = str(layer_dirs["gold"])
    wd.DB_PATH = str(layer_dirs["warehouse"] / "test_schema_migration.duckdb")
    wd.TABLE_NAME = "market_features"

    gold_file = layer_dirs["gold"] / "market_data_20260401_000001.csv"
    _write_gold_csv(gold_file, sample_gold_df)

    with duckdb.connect(wd.DB_PATH) as con:
        con.execute(
            f"""
            CREATE TABLE {wd.TABLE_NAME} (
                date VARCHAR,
                close DOUBLE,
                high DOUBLE,
                low DOUBLE,
                open DOUBLE,
                volume DOUBLE,
                symbol VARCHAR,
                close_1 DOUBLE,
                high_1 DOUBLE,
                low_1 DOUBLE,
                open_1 DOUBLE,
                volume_1 DOUBLE,
                close_2 DOUBLE,
                high_2 DOUBLE,
                low_2 DOUBLE,
                open_2 DOUBLE,
                volume_2 DOUBLE,
                close_3 DOUBLE,
                high_3 DOUBLE,
                low_3 DOUBLE,
                open_3 DOUBLE,
                volume_3 DOUBLE,
                close_4 DOUBLE,
                high_4 DOUBLE,
                low_4 DOUBLE,
                open_4 DOUBLE,
                volume_4 DOUBLE,
                return DOUBLE,
                ma_5 DOUBLE,
                volatility_5 DOUBLE
            )
            """
        )
        con.execute(f"INSERT INTO {wd.TABLE_NAME} VALUES ('2026-04-01', 1, 1, 1, 1, 1, 'AAPL', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")

    wd.load_data()

    with duckdb.connect(wd.DB_PATH, read_only=True) as con:
        columns = [row[1] for row in con.execute(f"PRAGMA table_info('{wd.TABLE_NAME}')").fetchall()]
        legacy_tables = con.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_name LIKE '{wd.TABLE_NAME}_legacy_%'"
        ).fetchall()
        count = con.execute(f"SELECT COUNT(*) FROM {wd.TABLE_NAME}").fetchone()[0]

    assert columns == ["date", "symbol", "open", "high", "low", "close", "volume", "return", "ma_5", "volatility_5"]
    assert len(legacy_tables) == 1
    assert count > 0
