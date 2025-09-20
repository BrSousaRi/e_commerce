import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()


# Conexao local Windows para Docker
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
user = os.getenv("POSTGRES_USER") 
password = os.getenv("POSTGRES_PASSWORD")
database = os.getenv("POSTGRES_DATABASE")

connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)

try:
    engine = create_engine(connection_string)
    conn = engine.connect()
    conn.execute(text("SELECT 1"))
    conn.close()
    print("OK - Conexao funcionando")
except Exception as e:
    print(f"ERRO - Conexao falhou: {e}")
