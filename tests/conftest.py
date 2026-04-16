from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def sample_silver_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-04-01",
                "symbol": "AAPL",
                "open": 100.0,
                "high": 102.0,
                "low": 99.5,
                "close": 101.5,
                "volume": 1000000,
            },
            {
                "date": "2026-04-02",
                "symbol": "AAPL",
                "open": 101.6,
                "high": 103.0,
                "low": 101.1,
                "close": 102.4,
                "volume": 900000,
            },
            {
                "date": "2026-04-01",
                "symbol": "MSFT",
                "open": 200.0,
                "high": 202.0,
                "low": 198.8,
                "close": 201.2,
                "volume": 800000,
            },
        ]
    )


@pytest.fixture
def sample_gold_df(sample_silver_df: pd.DataFrame) -> pd.DataFrame:
    gold_df = sample_silver_df.copy()
    gold_df["return"] = [None, 0.008866995, None]
    gold_df["ma_5"] = [None, None, None]
    gold_df["volatility_5"] = [None, None, None]
    return gold_df


@pytest.fixture
def layer_dirs(tmp_path: Path) -> dict[str, Path]:
    bronze = tmp_path / "data" / "bronze"
    silver = tmp_path / "data" / "silver"
    gold = tmp_path / "data" / "gold"
    warehouse = tmp_path / "warehouse"

    for directory in (bronze, silver, gold, warehouse):
        directory.mkdir(parents=True, exist_ok=True)

    return {"bronze": bronze, "silver": silver, "gold": gold, "warehouse": warehouse}
