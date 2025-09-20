import pandas as pd
import os
from conexaodb import engine

def upload_csvs():
    """Upload dos arquivos CSV para PostgreSQL"""
    try:
        # Usar a engine importada
        caminho = os.path.dirname(os.getcwd())
        pasta = "seeds"
        pasta_completa = os.path.join(caminho, pasta)

        arquivos_csv = [f for f in os.listdir(pasta_completa) if f.endswith(".csv")]
        print(f"Arquivos CSV: {arquivos_csv}")

        for arquivo in arquivos_csv:
            file_path = os.path.join(pasta_completa, arquivo)
            df_name = arquivo.replace(".csv", "")

            print(f"Processando: {arquivo}")
            df = pd.read_csv(file_path, encoding="utf-8")

            df.to_sql(
                name=df_name,
                con=engine,
                if_exists="replace",
                index=False,
            )
            print(f"SUCESSO: {df_name}")

    except Exception as e:
        print(f"ERRO: {e}")


if __name__ == "__main__":
    upload_csvs()
