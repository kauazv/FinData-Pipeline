import pandas as pd
import os

SILVER_PATH = "data/silver"
REQUIRED_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume"]


def get_latest_file():
    files = [f for f in os.listdir(SILVER_PATH) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado na camada silver")
    files.sort()
    return os.path.join(SILVER_PATH, files[-1])


def validate_data():

    file_path = get_latest_file()

    df = pd.read_csv(file_path)

    print("Iniciando validação de dados...\n")

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
        print("Falhas de validação encontradas:")
        for error in validation_errors:
            print(f"- {error}")
        print("\nValidação finalizada com erro.")
        raise SystemExit(1)
    else:
        print("Validação concluída com sucesso.")

    print("\nValidação finalizada.")


if __name__ == "__main__":
    validate_data()
