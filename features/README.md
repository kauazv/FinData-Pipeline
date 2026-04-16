# Features Module Roadmap

This folder is reserved for reusable feature engineering components.

Planned scope:

- central feature definitions (momentum, rolling volatility, drawdown)
- feature schema/version metadata
- train/inference feature parity checks

Current project state:

- baseline features are currently generated in `processing/create_gold_dataset.py`
- next step is to extract them into modular feature builders in this folder
