from src.extract.extrai_dados_api import main as extract

def main():
    extract()
    transform()
    load()
    logs()
    
if __name__ == "__main__":
    main()