# Architecture Decisions (ADR-lite)

## ADR-001: Layered Data Pipeline (Bronze/Silver/Gold)

- Decision: enforce layered data processing.
- Why: isolate raw ingestion issues from curated/feature-ready datasets.
- Tradeoff: more files and transformations to manage.

## ADR-002: DuckDB as Local Analytical Warehouse

- Decision: use DuckDB file-based warehouse for analytics.
- Why: lightweight, SQL-native, easy local reproducibility.
- Tradeoff: file lock concerns on some Windows environments.

## ADR-003: Validation Gates in Silver

- Decision: hard-fail pipeline on critical quality checks.
- Why: avoid silently propagating bad datasets to gold/ML.
- Tradeoff: stricter behavior can stop runs more often during early experimentation.

## ADR-004: Idempotent Warehouse Load by Business Key

- Decision: incremental insert keyed on `(date, symbol)`.
- Why: prevent duplicate analytical rows and support re-runs.
- Tradeoff: requires schema compatibility handling (implemented with auto-backup/recreate).

## ADR-005: Temporal Split for ML Baseline

- Decision: train/test split by date rather than random split.
- Why: reduce time-leakage risk for financial forecasting setup.
- Tradeoff: less training data flexibility vs. better realism.

## ADR-006: Baseline Pairing (Classification + Regression)

- Decision: support both direction classification and next-day return regression.
- Why: gives portfolio evidence of modeling choices and objective tradeoffs.
- Tradeoff: more artifacts and metrics to track.
