import os, sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyspark
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)

import httpx
from dotenv import load_dotenv
from minio import Minio
import pandas as pd
# from utils.files import limpa_pasta
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

load_dotenv()

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = (
    SparkSession.builder
        .appName("etl_ecommerce")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

def extrai_dados(
    minio_endpoint: str, 
    minio_access: str, 
    minio_secret: str, 
    bucket: str,
    pasta_minio: str,
    url: str):
    try:
        client = Minio(
            minio_endpoint.replace("http://", ""),
            minio_access,
            minio_secret,
            secure=False
        )
    except Exception as e:
        print(f"\nCredenciais erradas para o minio {e}\n")
        
    offset = 0
    limit = 1000
    df_list = []
    with httpx.Client(timeout=60.0) as http:
        try:
            while True:
                res = http.get(url, params={"limit": limit, "offset":offset})
                res.raise_for_status()
                dados = res.json()
                
                if not dados:
                    print("Fim dos dados")
                    break
                
                df_pandas = pd.DataFrame(dados)
                df_list.append(df_pandas)
                
                offset+=limit
        except Exception as e:
            print(f"\nErro ao extrair dados clientes: {e}\n")
    
    bronze_path = f"s3a://{bucket}/bronze/{pasta_minio}"
    df = spark.createDataFrame(df_list)
    
    df.write.format("parquet").mode("overwrite").option("mergeSchema", "true").save(bronze_path)

def extrai_dados_clientes():
    url_api = os.getenv("URL_BASE") + "/customers"
    extrai_dados(
        os.getenv("MINIO_ENDPOINT"), 
        os.getenv("MINIO_ACCESS_KEY"), 
        os.getenv("MINIO_SECRET_KEY"),
        "bronze",
        "/customers",
        url_api
        )
    
def extrai_dados_localizacao():
    url_api = os.getenv("URL_BASE") + "/location"
    extrai_dados(
        os.getenv("MINIO_ENDPOINT"), 
        os.getenv("MINIO_ACCESS_KEY"), 
        os.getenv("MINIO_SECRET_KEY"),
        "bronze",
        "/location",
        url_api
        )
    
def extrai_dados_itens_pedidos():
    url_api = os.getenv("URL_BASE") + "/order_items"
    extrai_dados(
        os.getenv("MINIO_ENDPOINT"), 
        os.getenv("MINIO_ACCESS_KEY"), 
        os.getenv("MINIO_SECRET_KEY"),
        "bronze",
        "/order_items",
        url_api
        )
    
def extrai_dados_pagamentos():
    url_api = os.getenv("URL_BASE") + "/payments"
    extrai_dados(
        os.getenv("MINIO_ENDPOINT"), 
        os.getenv("MINIO_ACCESS_KEY"), 
        os.getenv("MINIO_SECRET_KEY"),
        "bronze",
        "/payments",
        url_api
        )
    
def extrai_dados_avaliacoes_pedidos():
    url_api = os.getenv("URL_BASE") + "/reviews"
    extrai_dados(
        os.getenv("MINIO_ENDPOINT"), 
        os.getenv("MINIO_ACCESS_KEY"), 
        os.getenv("MINIO_SECRET_KEY"),
        "bronze",
        "/reviews",
        url_api
        )
    
def extrai_dados_pedidos():
    url_api = os.getenv("URL_BASE") + "/orders"
    extrai_dados(
        os.getenv("MINIO_ENDPOINT"), 
        os.getenv("MINIO_ACCESS_KEY"), 
        os.getenv("MINIO_SECRET_KEY"),
        "bronze",
        "/orders",
        url_api
        )
    
def extrai_dados_produtos():
    url_api = os.getenv("URL_BASE") + "/products"
    extrai_dados(
        os.getenv("MINIO_ENDPOINT"), 
        os.getenv("MINIO_ACCESS_KEY"), 
        os.getenv("MINIO_SECRET_KEY"),
        "bronze",
        "/products",
        url_api
        )
    
def extrai_dados_vendedores():
    url_api = os.getenv("URL_BASE") + "/sellers"
    extrai_dados(
        os.getenv("MINIO_ENDPOINT"), 
        os.getenv("MINIO_ACCESS_KEY"), 
        os.getenv("MINIO_SECRET_KEY"),
        "bronze",
        "/sellers",
        url_api
        )

def extrai_dados_product_category():
    url_api = os.getenv("URL_BASE") + "/product_category"
    extrai_dados(
        os.getenv("MINIO_ENDPOINT"), 
        os.getenv("MINIO_ACCESS_KEY"), 
        os.getenv("MINIO_SECRET_KEY"),
        "bronze",
        "/sellers",
        url_api
        )

def main():
    extrai_dados_clientes()
    spark.stop()
    
if __name__ == "__main__":
    main()