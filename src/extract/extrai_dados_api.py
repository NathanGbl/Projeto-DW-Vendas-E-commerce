import os
import httpx
from dotenv import load_dotenv
from minio import Minio
import pandas as pd
from pyspark.sql import SparkSession

load_dotenv()

def cria_spark_session(
    minio_endpoint: str, 
    minio_access: str, 
    minio_secret: str, 
    is_endpoint_https: bool, 
    app_name: str) -> SparkSession:
    
    spark = (
        SparkSession.builder
            .appName(app_name)
            # Adicione os JARs do S3A
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_access)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if is_endpoint_https else "false")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            # Delta Lake
            # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
    )
    
    print("config2")
    return spark

def extrai_dados(
    spark: SparkSession,
    minio_endpoint: str, 
    minio_access: str, 
    minio_secret: str, 
    bucket: str,
    pasta_minio: str,
    url: str):
    print("Client")
    try:
        client = Minio(
            minio_endpoint.replace("http://", ""),
            minio_access,
            minio_secret,
            secure=minio_endpoint.startswith("https")
        )
    except Exception as e:
        print(f"\nCredenciais erradas para o minio {e}\n")
        return
        
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        
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
            return
    
    if not df_list:
        return
    
    bronze_path = f"s3a://{bucket}/bronze/{pasta_minio}"
    df_concatenado = pd.concat(df_list, ignore_index=True)
    df = spark.createDataFrame(df_concatenado)
    
    df.write.format("parquet").mode("overwrite").option("mergeSchema", "true").save(bronze_path)

def extrai_dados_clientes(
    spark: SparkSession,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str
):
    print("Clientes")
    endpoint_dados = "customers"
    url_api = os.getenv("URL_BASE") + "/customers"
    extrai_dados(
        spark,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        "datalake",
        endpoint_dados,
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
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET = os.getenv("MINIO_SECRET_KEY")
        
    spark = cria_spark_session(
        MINIO_ENDPOINT, 
        MINIO_ACCESS, 
        MINIO_SECRET, 
        False, 
        "extract_ecommerce")
    extrai_dados_clientes(spark,
        MINIO_ENDPOINT, 
        MINIO_ACCESS, 
        MINIO_SECRET)
    
    spark.stop()
    
if __name__ == "__main__":
    main()