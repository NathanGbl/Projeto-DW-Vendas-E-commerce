import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def nova_conexao(
    server:str,
    banco:str
    ):
    user, pwd = os.getenv("USER_SERVER"), os.getenv("PASS_SERVER")
    try:
        params = quote_plus(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={banco};"
            f"UID={user};"
            f"PWD={pwd};"
            f"Trusted_Connection=yes;"
            f"TrusServerCertificate=yes;"
            f"Encrypt=yes;"
        )
        engine = create_engine(
            f"mssql+pyodbc://?odbc_connect={params}"
        )
        
        return engine
    except Exception as e:
        print(f"Erro ao conectar: {e}")
    