import pandas as pd
import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

SILVER_PATH = os.getenv("SILVER_PATH", "data/silver")
REQUIRED_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume"]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("validation.validate_market_data")


def get_latest_file():
    files = [f for f in os.listdir(SILVER_PATH) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado na camada silver")
    files.sort()
    return os.path.join(SILVER_PATH, files[-1])


def validate_data():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    file_path = get_latest_file()

    df = pd.read_csv(file_path)

    logger.info("Iniciando validação de dados | run_id=%s | arquivo=%s", run_id, file_path)

    validation_errors = []

    if df.empty:
        validation_errors.append("Dataset silver está vazio")

    # verificar colunas obrigatórias
    missing_columns = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_columns:
        validation_errors.append(f"Colunas obrigatórias ausentes: {missing_columns}")

    if not missing_columns:
        # verificar valores nulos nas colunas obrigatórias
        nulls = df[REQUIRED_COLUMNS].isnull().sum()
        if nulls.sum() > 0:
            validation_errors.append(f"Valores nulos detectados: {nulls[nulls > 0].to_dict()}")

        # verificar duplicados de chave de negócio
        duplicates = df.duplicated(subset=["date", "symbol"]).sum()
        if duplicates > 0:
            validation_errors.append(f"{duplicates} registros duplicados para chave (date, symbol)")

        numeric_columns = ["open", "high", "low", "close", "volume"]
        for column in numeric_columns:
            coerced = pd.to_numeric(df[column], errors="coerce")
            if coerced.isnull().any():
                validation_errors.append(f"Coluna '{column}' possui valores não numéricos")

        negative_volume = (pd.to_numeric(df["volume"], errors="coerce") < 0).sum()
        if negative_volume > 0:
            validation_errors.append(f"{negative_volume} registros com volume negativo")

    if validation_errors:
        logger.error("Falhas de validação encontradas | run_id=%s", run_id)
        for error in validation_errors:
            logger.error("- %s", error)
        logger.error("Validação finalizada com erro | run_id=%s", run_id)
        raise SystemExit(1)
    else:
        logger.info("Validação concluída com sucesso | run_id=%s | linhas=%d", run_id, len(df))


if __name__ == "__main__":
    validate_data()
