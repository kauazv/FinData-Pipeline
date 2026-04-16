from pathlib import Path

import pandas as pd

from processing import process_market_data as pm


def _write_legacy_bronze_csv(path: Path) -> None:
    columns = pd.MultiIndex.from_tuples(
        [
            ("Date", "Unnamed: 0_level_1"),
            ("Close", "AAPL"),
            ("High", "AAPL"),
            ("Low", "AAPL"),
            ("Open", "AAPL"),
            ("Volume", "AAPL"),
            ("symbol", "Unnamed: 6_level_1"),
            ("Close", "MSFT"),
            ("High", "MSFT"),
            ("Low", "MSFT"),
            ("Open", "MSFT"),
            ("Volume", "MSFT"),
        ]
    )
    df = pd.DataFrame(
        [
            ["2026-04-01", 101.0, 102.0, 100.0, 100.5, 1000, "AAPL", None, None, None, None, None],
            ["2026-04-01", None, None, None, None, None, "MSFT", 201.0, 203.0, 199.0, 200.5, 2000],
            ["2026-04-02", 102.0, 103.0, 101.0, 101.5, 1100, "AAPL", None, None, None, None, None],
            ["2026-04-02", None, None, None, None, None, "MSFT", 202.0, 204.0, 200.0, 201.5, 2100],
            ["2026-04-02", 102.0, 103.0, 101.0, 101.5, 1100, "AAPL", None, None, None, None, None],
        ],
        columns=columns,
    )
    df.to_csv(path, index=False)


def test_process_data_normalizes_legacy_and_deduplicates(layer_dirs):
    bronze_file = layer_dirs["bronze"] / "market_data_20260401_000000.csv"
    _write_legacy_bronze_csv(bronze_file)

    pm.BRONZE_PATH = str(layer_dirs["bronze"])
    pm.SILVER_PATH = str(layer_dirs["silver"])

    pm.process_data()

    output_file = layer_dirs["silver"] / bronze_file.name
    assert output_file.exists()

    result = pd.read_csv(output_file)
    assert result.columns.tolist() == pm.REQUIRED_COLUMNS
    assert len(result) == 4
    assert set(result["symbol"].unique()) == {"AAPL", "MSFT"}
    assert result.duplicated(subset=["date", "symbol"]).sum() == 0


def test_load_bronze_uses_canonical_schema(layer_dirs):
    bronze_file = layer_dirs["bronze"] / "market_data_20260401_000001.csv"
    canonical_df = pd.DataFrame(
        [
            {
                "date": "2026-04-01",
                "symbol": "AAPL",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1000,
                "ignored_column": "x",
            }
        ]
    )
    canonical_df.to_csv(bronze_file, index=False)

    loaded = pm.load_bronze(str(bronze_file))
    assert loaded.columns.tolist() == pm.REQUIRED_COLUMNS
    assert len(loaded) == 1
