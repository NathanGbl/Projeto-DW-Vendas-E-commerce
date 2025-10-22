import os

from dotenv import load_dotenv
from pandas import DataFrame
from func.conn import nova_conexao

def carrega_base(
    banco: str,
    tabela: str,
    dados: DataFrame
):
    load_dotenv()
    server = os.getenv("SERVER")
    with nova_conexao(server, banco).connect() as engine:
        dados.to_sql(
            name=tabela,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000
        )