from sqlalchemy import create_engine

def nova_conexao(
    user:str,
    senha:str,
    servidor:str,
    porta:str,
    driver:str,
    nome_banco:str
    ):
    create_engine(
        f"mssql+pyodbc://{user}:{senha}@{servidor}:{porta}/{nome_banco}?driver={driver}"
    )