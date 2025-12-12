import os
from func.conn import nova_conexao
from src.extract.extrai_dados_api import main as extract
import src.api.routes as api
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

def main():
    try:
        engine = nova_conexao(os.getenv("SERVER"), os.getenv("OLTP"))
        
        # with engine.connect() as conn:
        #     conn.execute(text("SELECT 1"))
        print("DEU BOM")
    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
if __name__ == "__main__":
    main()