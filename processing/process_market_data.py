import pandas as pd
import os

BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"

os.makedirs(SILVER_PATH, exist_ok=True)


def get_latest_file():
    files = [f for f in os.listdir(BRONZE_PATH) if f.endswith(".csv")]
    files.sort()
    return os.path.join(BRONZE_PATH, files[-1])


def process_data():

    file_path = get_latest_file()

    df = pd.read_csv(file_path)

    # padronização de nomes
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # remover duplicados
    df = df.drop_duplicates()

    # tratar valores nulos
    df = df.dropna()

    output_file = file_path.replace("bronze", "silver")

    df.to_csv(output_file, index=False)

    print(f"Arquivo processado salvo em: {output_file}")


if __name__ == "__main__":
    process_data()