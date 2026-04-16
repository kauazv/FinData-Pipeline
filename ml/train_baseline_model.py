import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

load_dotenv()

GOLD_PATH = Path(os.getenv("GOLD_PATH", "data/gold"))
ML_ARTIFACTS_PATH = Path(os.getenv("ML_ARTIFACTS_PATH", "ml/artifacts"))
ML_TEST_RATIO = float(os.getenv("ML_TEST_RATIO", "0.2"))
MIN_TRAIN_ROWS = int(os.getenv("ML_MIN_TRAIN_ROWS", "100"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ml.train_baseline_model")


def get_latest_gold_file() -> Path:
    files = sorted([f for f in GOLD_PATH.glob("market_data_*.csv") if f.is_file()])
    if not files:
        raise FileNotFoundError(f"Nenhum CSV gold encontrado em: {GOLD_PATH}")
    return files[-1]


def build_supervised_dataset(gold_df: pd.DataFrame) -> pd.DataFrame:
    df = gold_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "symbol", "open", "high", "low", "close", "volume"])
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

    # Features de preço/volume (todas até t)
    df["hl_spread"] = (df["high"] - df["low"]) / df["close"]
    df["oc_change"] = (df["close"] - df["open"]) / df["open"]
    df["volume_change_1d"] = df.groupby("symbol")["volume"].pct_change()
    df["return_lag_1"] = df.groupby("symbol")["return"].shift(1)
    df["return_lag_2"] = df.groupby("symbol")["return"].shift(2)

    # Targets D+1: direção e valor do retorno
    df["target_return_next_day"] = df.groupby("symbol")["return"].shift(-1)
    df["target_up"] = (df["target_return_next_day"] > 0).astype(int)

    model_columns = [
        "date",
        "symbol",
        "return",
        "ma_5",
        "volatility_5",
        "hl_spread",
        "oc_change",
        "volume_change_1d",
        "return_lag_1",
        "return_lag_2",
        "target_return_next_day",
        "target_up",
    ]
    df = df[model_columns]
    df = df.dropna().reset_index(drop=True)
    return df


def temporal_split(df: pd.DataFrame, test_ratio: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    unique_dates = sorted(df["date"].unique())
    if len(unique_dates) < 2:
        raise ValueError("Datas insuficientes para split temporal")

    cutoff_index = int(len(unique_dates) * (1 - test_ratio))
    cutoff_index = max(1, min(cutoff_index, len(unique_dates) - 1))
    cutoff_date = unique_dates[cutoff_index]

    train_df = df[df["date"] < cutoff_date].copy()
    test_df = df[df["date"] >= cutoff_date].copy()
    return train_df, test_df


def evaluate_classifier(model, x_train, y_train, x_test, y_test) -> dict:
    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)
    return {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_pred, zero_division=0)),
        "f1": float(f1_score(y_test, y_pred, zero_division=0)),
    }


def evaluate_regressor(model, x_train, y_train, x_test, y_test) -> dict:
    model.fit(x_train, y_train)
    y_pred = model.predict(x_test)
    rmse = mean_squared_error(y_test, y_pred) ** 0.5
    return {
        "mae": float(mean_absolute_error(y_test, y_pred)),
        "rmse": float(rmse),
        "r2": float(r2_score(y_test, y_pred)),
    }


def train_and_save() -> None:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info("Iniciando treino baseline | run_id=%s", run_id)

    gold_file = get_latest_gold_file()
    gold_df = pd.read_csv(gold_file)
    supervised_df = build_supervised_dataset(gold_df)

    if len(supervised_df) < MIN_TRAIN_ROWS:
        raise ValueError(
            f"Dados insuficientes para treino. Linhas={len(supervised_df)} | mínimo={MIN_TRAIN_ROWS}"
        )

    train_df, test_df = temporal_split(supervised_df, ML_TEST_RATIO)
    if train_df.empty or test_df.empty:
        raise ValueError("Split temporal inválido: treino ou teste vazio")

    feature_columns = [
        "return",
        "ma_5",
        "volatility_5",
        "hl_spread",
        "oc_change",
        "volume_change_1d",
        "return_lag_1",
        "return_lag_2",
    ]

    x_train = train_df[feature_columns]
    x_test = test_df[feature_columns]

    y_train_cls = train_df["target_up"]
    y_test_cls = test_df["target_up"]
    y_train_reg = train_df["target_return_next_day"]
    y_test_reg = test_df["target_return_next_day"]

    cls_models = {
        "logistic_regression": Pipeline(
            [
                ("scaler", StandardScaler()),
                ("model", LogisticRegression(max_iter=1000, random_state=42)),
            ]
        ),
        "random_forest": RandomForestClassifier(
            n_estimators=300,
            max_depth=8,
            min_samples_leaf=5,
            random_state=42,
        ),
    }

    reg_models = {
        "ridge_regression": Pipeline(
            [
                ("scaler", StandardScaler()),
                ("model", Ridge(alpha=1.0)),
            ]
        ),
        "random_forest_regressor": RandomForestRegressor(
            n_estimators=300,
            max_depth=8,
            min_samples_leaf=5,
            random_state=42,
        ),
    }

    cls_results = {}
    best_cls_name = None
    best_cls_f1 = -1.0
    best_cls_model = None

    for name, model in cls_models.items():
        metrics = evaluate_classifier(model, x_train, y_train_cls, x_test, y_test_cls)
        cls_results[name] = metrics
        if metrics["f1"] > best_cls_f1:
            best_cls_f1 = metrics["f1"]
            best_cls_name = name
            best_cls_model = model

    reg_results = {}
    best_reg_name = None
    best_reg_rmse = float("inf")
    best_reg_model = None

    for name, model in reg_models.items():
        metrics = evaluate_regressor(model, x_train, y_train_reg, x_test, y_test_reg)
        reg_results[name] = metrics
        if metrics["rmse"] < best_reg_rmse:
            best_reg_rmse = metrics["rmse"]
            best_reg_name = name
            best_reg_model = model

    ML_ARTIFACTS_PATH.mkdir(parents=True, exist_ok=True)

    metrics_payload = {
        "run_id": run_id,
        "source_gold_file": str(gold_file),
        "dataset_rows": int(len(supervised_df)),
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "feature_columns": feature_columns,
        "classification": {
            "models": cls_results,
            "best_model": best_cls_name,
        },
        "regression": {
            "models": reg_results,
            "best_model": best_reg_name,
        },
    }
    metrics_path = ML_ARTIFACTS_PATH / "metrics_baseline.json"
    metrics_path.write_text(json.dumps(metrics_payload, indent=2), encoding="utf-8")

    cls_model_path = ML_ARTIFACTS_PATH / "best_model_classifier.joblib"
    reg_model_path = ML_ARTIFACTS_PATH / "best_model_regressor.joblib"
    joblib.dump(best_cls_model, cls_model_path)
    joblib.dump(best_reg_model, reg_model_path)

    dataset_path = ML_ARTIFACTS_PATH / "supervised_dataset_latest.csv"
    supervised_df.to_csv(dataset_path, index=False)

    logger.info(
        "Treino concluído | run_id=%s | best_cls=%s | f1=%.4f | best_reg=%s | rmse=%.6f | metrics=%s",
        run_id,
        best_cls_name,
        cls_results[best_cls_name]["f1"],
        best_reg_name,
        reg_results[best_reg_name]["rmse"],
        metrics_path,
    )


if __name__ == "__main__":
    train_and_save()
