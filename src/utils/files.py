import os
import glob

def limpa_pasta(pasta:str, extensao:str) -> None:
    arquivos_xlsx = glob.glob(os.path.join(pasta, f"*{extensao}"))
    for arquivo in arquivos_xlsx:
        try:
            os.remove(arquivo)
            print(f"Removido: {arquivo}")
        except Exception as e:  # noqa: BLE001
            print(f"Erro ao remover {arquivo}: {e}")