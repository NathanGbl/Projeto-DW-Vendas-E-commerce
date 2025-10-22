import os
import httpx
from dotenv import load_dotenv
from minio import Minio
import pandas as pd
from sqlalchemy import text
from utils.database import carrega_base
from utils.files import limpa_pasta
from func.conn import nova_conexao
from datetime import datetime

load_dotenv()

def extrai_dados(
    minio_endpoint: str, 
    minio_access: str, 
    minio_secret: str, 
    bucket: str,
    pasta: str,
    arquivo: str,
    url: str):
    client_minio = Minio(
        minio_endpoint,
        minio_access,
        minio_secret,
    )
    with httpx.Client(timeout=60.0) as client:
        offset = 0
        limit = 1000
        try:
            while True:
                res = client.get(url, params={"limit": limit, "offset":offset})
                if res.status_code != 200:
                    print(f"Erro: {res.status_code} => {res.text}")
                    break
                
                dados = res.json()
                
                if not dados:
                    print("Fim dos dados")
                    break
                
                df = pd.DataFrame(dados)
                data_hoje = datetime.now().strftime("YYYYmmdd")
                df.to_parquet(f"data/bronze/{pasta}/{arquivo}_{data_hoje}.parquet")
                offset+=limit
        except Exception as e:
            print(f"Erro ao extrair dados clientes: {e}")

def extrai_dados_clientes():
    url_api = os.getenv("URL_BASE") + "/customers"
    database = os.getenv("ODS")
    server = os.getenv("SERVER")
    table_name = "FILE_CLIENTES"
    
    extrai_dados()
    
def extrai_dados_localizacao():
    url_api = os.getenv("URL_BASE") + "/location"
    database = os.getenv("ODS")
    server = os.getenv("SERVER")
    table_name = "FILE_LOCALIZACAO"
    
    extrai_dados(server, database, table_name, url_api)
    
def extrai_dados_itens_pedidos():
    url_api = os.getenv("URL_BASE") + "/order_items"
    database = os.getenv("ODS")
    server = os.getenv("SERVER")
    table_name = "FILE_ITENS_PEDIDOS"
    
    extrai_dados(server, database, table_name, url_api)
    
def extrai_dados_pagamentos():
    url_api = os.getenv("URL_BASE") + "/payments"
    database = os.getenv("ODS")
    server = os.getenv("SERVER")
    table_name = "FILE_PAGAMENTOS"
    
    extrai_dados(server, database, table_name, url_api)
    
def extrai_dados_avaliacoes_pedidos():
    url_api = os.getenv("URL_BASE") + "/reviews"
    database = os.getenv("ODS")
    server = os.getenv("SERVER")
    table_name = "FILE_REVIEWS"
    
    extrai_dados(server, database, table_name, url_api)
    
def extrai_dados_pedidos():
    url_api = os.getenv("URL_BASE") + "/orders"
    database = os.getenv("ODS")
    server = os.getenv("SERVER")
    table_name = "FILE_PEDIDOS"
    
    extrai_dados(server, database, table_name, url_api)
    
def extrai_dados_produtos():
    url_api = os.getenv("URL_BASE") + "/products"
    database = os.getenv("ODS")
    server = os.getenv("SERVER")
    table_name = "FILE_PRODUTOS"
    
    extrai_dados(server, database, table_name, url_api)
    
def extrai_dados_vendedores():
    url_api = os.getenv("URL_BASE") + "/sellers"
    database = os.getenv("ODS")
    server = os.getenv("SERVER")
    table_name = "FILE_VENDEDORES"
    
    extrai_dados(server, database, table_name, url_api)

def extrai_dados_product_category():
    url_api = os.getenv("URL_BASE") + "/product_category"
    database = os.getenv("ODS")
    server = os.getenv("SERVER")
    table_name = "FILE_PRODUCT_CATEGORY"
    
    extrai_dados(server, database, table_name, url_api)

def main():
    limpa_pasta("data", "")
    extrai_dados_clientes()
    
if __name__ == "__main__":
    main()