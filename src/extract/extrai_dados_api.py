import os
import httpx
from dotenv import load_dotenv
import pandas as pd
from src.utils.envia_dados_banco import carrega_base

load_dotenv()
def extrai_dados_clientes():
    url_base = os.getenv("URL_BASE")
    url_api = url_base + "/customers"
    banco = os.getenv("ODS")
    tabela = "FILE_BASE_HISTORICA_CLIENTES"

    with httpx.Client(timeout=60.0) as client:
        offset = 0
        limit = 1000
        try:
            while True:
                res = client.get(url_api, params={"limit": limit, "offset":offset})
                if res.status_code != 200:
                    print(f"Erro: {res.status_code} => {res.text}")
                    break
                dados = res.json()
                if not dados:
                    print("Fim dos dados")
                    break
                df = pd.DataFrame(dados)
                offset+=limit
                carrega_base(banco, tabela, df)
        except Exception as e:
            print(f"Erro ao extrair dados clientes: {e}")
            
def main():
    extrai_dados_clientes()