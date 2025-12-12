import os
import sqlalchemy
from urllib.parse import quote_plus

def nova_conexao(
    server:str,
    banco:str
    ) -> sqlalchemy.Engine:
    user, pwd = os.getenv("USER_SERVER"), os.getenv("PASS_SERVER")
    
    try:
        params = quote_plus(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={banco};"
            f"UID={user};"
            f"PWD={pwd};"
            # f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
            f"Encrypt=yes;"
        )
        engine = sqlalchemy.create_engine(
            f"mssql+pyodbc://?odbc_connect={params}",
            pool_pre_ping=True
        )
        
        return engine
    except Exception as e:
        print(f"Erro ao conectar: {e}")
    