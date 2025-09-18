from sqlalchemy import create_engine

def nova_conexao(
    server:str,
    banco:str
    ):
    try:
        engine = create_engine(
            f"mssql+pyodbc://@{server}/{banco}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
    except Exception as e:
        print(f"Erro ao conectar: {e}")
    return engine