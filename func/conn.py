from sqlalchemy import create_engine
import pyodbc

def nova_conexao(
    user:str,
    senha:str,
    servidor:str,
    nome_banco:str
    ):
    try:
        engine = create_engine(
            f"mssql+pyodbc://{user}:{senha}@{servidor}/{nome_banco}?driver=ODBC+Driver+17+for+SQL+Server"
        )
    except Exception as e:
        print(f"Erro ao conectar: {e}")
    return engine