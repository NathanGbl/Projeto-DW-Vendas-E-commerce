import os
import kagglehub
import pandas as pd
import sys
pasta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, pasta_raiz)
from func.conn import nova_conexao  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
import pyodbc  # noqa: E402

load_dotenv()

def baixa_arquivos(titulo_dataset: str):
  datasets = kagglehub.dataset_download(titulo_dataset, force_download=True)
  return datasets

def insere_arquivo(caminho_arquivo:str):
  download_dir = baixa_arquivos(caminho_arquivo)
  print(download_dir)
  
  server = os.getenv("SERVER")
  banco = os.getenv("OLTP")
  
  engine = nova_conexao(
    server,
    banco
    )
  
  for file in os.listdir(download_dir):
    if file.endswith(".csv"):
      df = pd.read_csv(
        os.path.join(download_dir, file),
        encoding="latin1",
        on_bad_lines="skip"
        )
      file_name = file.replace(".csv", "")
      try:
        df.to_sql(
          name = file_name,
          con = engine,
          if_exists = "append",
          index = False,
          chunksize = 1000
        )
      except Exception as e:
        print(f"Erro ao inserir {file_name}: {e}")
      
if __name__ == "__main__":
  insere_arquivo("olistbr/brazilian-ecommerce")
  # print(pyodbc.drivers())