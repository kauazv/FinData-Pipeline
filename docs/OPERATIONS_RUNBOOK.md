# Operations Runbook

## Purpose

This runbook describes how to operate the FinData Pipeline in local/dev mode and how to troubleshoot common failures.

## Standard Commands

Install dependencies:

```bash
pip install -r requirements.txt
```

Run end-to-end with ML:

```bash
python pipelines/run_pipeline.py --with-ml
```

Run without ingestion (when external API/network is unavailable):

```bash
python pipelines/run_pipeline.py --skip-ingestion --with-ml
```

Run tests:

```bash
python -m pytest
```

## Key Outputs

- Run metadata: `storage/runs/run_<run_id>.json`
- Gold dataset: `data/gold/market_data_<timestamp>.csv`
- DuckDB tables: `warehouse/*.duckdb`
- ML artifacts:
  - `ml/artifacts/metrics_baseline.json`
  - `ml/artifacts/best_model_classifier.joblib`
  - `ml/artifacts/best_model_regressor.joblib`

## Quality Gates

The pipeline should fail if:

- silver dataset is empty
- required columns are missing
- duplicate `(date, symbol)` keys exist
- non-numeric values appear in OHLCV columns
- negative volume is detected

## Common Issues and Fixes

1. `Binder Error: table ... has N columns but M values were supplied`
- Cause: schema drift between old/new DuckDB table format.
- Current behavior: loader backs up legacy table and recreates compatible schema automatically.

2. `market_data.duckdb.wal: Access denied`
- Cause: file lock/permissions on Windows DuckDB file.
- Current behavior: loader falls back to timestamped database file and proceeds.

3. `Nenhum dado foi coletado da API`
- Cause: ingestion unavailable (network, yfinance cache issues, provider error).
- Mitigation: run with `--skip-ingestion` and use latest bronze/silver/gold snapshots.

## Production-Ready Next Steps

- Central scheduler (Airflow or cron with retries and alerting)
- Data contract checks (e.g., Great Expectations/Pandera)
- Model registry/versioning for ML artifacts
- Observability (structured logs + metrics dashboard)
