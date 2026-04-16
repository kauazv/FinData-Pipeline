import pandas as pd
import pytest

from validation import validate_market_data as vm


def test_validate_data_fails_for_empty_dataset(layer_dirs):
    vm.SILVER_PATH = str(layer_dirs["silver"])
    empty_file = layer_dirs["silver"] / "market_data_20260401_000000.csv"
    pd.DataFrame(columns=vm.REQUIRED_COLUMNS).to_csv(empty_file, index=False)

    with pytest.raises(SystemExit) as exc:
        vm.validate_data()

    assert exc.value.code == 1


def test_validate_data_fails_for_duplicate_business_key(layer_dirs, sample_silver_df):
    vm.SILVER_PATH = str(layer_dirs["silver"])
    dup_df = pd.concat([sample_silver_df, sample_silver_df.iloc[[0]]], ignore_index=True)
    file_path = layer_dirs["silver"] / "market_data_20260401_000001.csv"
    dup_df.to_csv(file_path, index=False)

    with pytest.raises(SystemExit) as exc:
        vm.validate_data()

    assert exc.value.code == 1


def test_validate_data_passes_for_clean_dataset(layer_dirs, sample_silver_df):
    vm.SILVER_PATH = str(layer_dirs["silver"])
    file_path = layer_dirs["silver"] / "market_data_20260401_000002.csv"
    sample_silver_df.to_csv(file_path, index=False)

    vm.validate_data()
