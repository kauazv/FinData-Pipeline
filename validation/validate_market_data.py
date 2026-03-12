import pandas as pd
import os

SILVER_PATH = "data/silver"


def get_latest_file():
    files = [f for f in os.listdir(SILVER_PATH) if f.endswith(".csv")]
    files.sort()
    return os.path.join(SILVER_PATH, files[-1])


def validate_data():

    file_path = get_latest_file()

    df = pd.read_csv(file_path)

    print("Iniciando validação de dados...\n")

    #verificar valores nulos
    nulls = df.isnull().sum()

    if nulls.sum() > 0:
        print("Existem valores nulos:")
        print(nulls)
    else:
        print("Nenhum valor nulo encontrado")

    #verificar duplicados
    duplicates = df.duplicated().sum()

    if duplicates > 0:
        print(f"{duplicates} registros duplicados encontrados")
    else:
        print("Nenhum registro duplicado")

    #verificar colunas obrigatórias
    required_columns = ["symbol", "price", "timestamp"]

    missing_columns = [c for c in required_columns if c not in df.columns]

    if missing_columns:
        print("Colunas obrigatórias ausentes:", missing_columns)
    else:
        print("Todas colunas obrigatórias presentes")

    print("\nValidação finalizada.")


if __name__ == "__main__":
    validate_data()