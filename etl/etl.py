from prefect import flow, task, get_run_logger
from pathlib import Path
import pandas as pd
import duckdb
import wget

@task(retries=3, retry_delay_seconds=5)
def download_csv(filename: str, save_directory: str, downloaded_csv_path: str, logger) -> None:
    """
    Baixa o arquivo CSV da URL especificada.

    Args:
        filename (str): O nome do arquivo CSV a ser baixado.
        save_directory (str): O diretório onde o arquivo será salvo.
        downloaded_csv_path:        
        logger: O logger utilizado para registrar eventos durante a execução.
    """
    logger.info(f"Iniciando download de {filename}...")
    download_url = 'https://drive.google.com/uc?export=download&id=1eoy8MlYin9PxbCjozT0kjPXPsq0RXEgY'
    logger.info(f"Downloading {filename} from {download_url} to {save_directory}")
    wget.download(download_url, out=str(downloaded_csv_path))
    logger.info(f"Download complete.")

@task
def to_pandas(filename: str, downloaded_csv_path: str, logger) -> pd.DataFrame:
    """
    Lê o arquivo CSV em um DataFrame pandas.

    Args:
        filename (str): O nome do arquivo CSV baixado.        
        downloaded_csv_path : O caminho para o arquivo CSV a ser lido.
        logger: O logger utilizado para registrar eventos durante a execução.

    Returns:
        pd.DataFrame: O DataFrame gerado a partir do arquivo CSV.
    """
    logger.info(f"Lendo {filename} em um DataFrame pandas...")
    df = pd.read_csv(downloaded_csv_path, sep=';', encoding='latin-1').reset_index().rename(columns={'index': 'index'})
    logger.info(f"Arquivo {filename} lido com sucesso.")
    logger.info(f"DataFrame gerado com shape {df.shape}")
    return df

@task
def create_table(df: pd.DataFrame, conn, logger) -> None:
    """
    Cria a tabela 'games_sales' no DuckDB a partir do DataFrame.

    Args:
        df (pd.DataFrame): O DataFrame a ser utilizado para criar a tabela.
        conn: A conexão com o banco de dados DuckDB.
        logger: O logger utilizado para registrar eventos durante a execução.
    """
    logger.info("Criando tabela 'games_sales' no DuckDB...")
    conn.sql("DROP TABLE IF EXISTS games_sales;")
    query = """
    CREATE TABLE games_sales AS
    SELECT 
        *,
        (Sales_NA + Sales_EU + Sales_JP + Sales_Others) AS Global_Sales
    FROM df;
    """
    conn.sql(query)
    logger.info("Tabela 'games_sales' criada com sucesso.")

@task
def format_sales_values(conn, logger) -> None:
    """
    Formata os valores de vendas na tabela 'games_sales'.

    Args:
        conn: A conexão com o banco de dados DuckDB.
        logger: O logger utilizado para registrar eventos durante a execução.
    """
    logger.info("Formatando valores de vendas na tabela 'games_sales'...")
    columns = ["Sales_NA", "Sales_EU", "Sales_JP", "Sales_Others", "Global_Sales"]
    alter_sql = "ALTER TABLE games_sales ALTER COLUMN $column_name SET DATA TYPE DOUBLE USING $column_name / 1000;"
    for column in columns:
        conn.sql(alter_sql.replace("$column_name", column))
    logger.info("Valores de vendas formatados com sucesso.")

@task
def load_to_csv(conn, logger) -> None:
    """
    Exporta os dados da tabela 'games_sales' para um arquivo CSV.

    Args:
        conn: A conexão com o banco de dados DuckDB.
        logger: O logger utilizado para registrar eventos durante a execução.
    """
    logger.info("Exportando dados da tabela 'games_sales' para um arquivo CSV...")
    conn.sql("""
    COPY games_sales TO 'data/games_sales.csv' (HEADER, DELIMITER ';')
    """)
    logger.info("Dados exportados com sucesso.")

@flow
def etl_games_sales() -> None:
    """
    Orquestra o fluxo de ETL para processar os dados de vendas de jogos.
    """
    logger = get_run_logger()
    save_directory = 'data'
    filename = 'BASE_DADOS.csv'
    downloaded_csv_path = Path('data/BASE_DADOS.csv')
    duckdb_path = Path('data/db_games_sales.db')
    conn = duckdb.connect(duckdb_path)
    logger.info("Iniciando pipeline ETL...")
    
    #EXTRACT:
    download_csv(filename, save_directory, downloaded_csv_path, logger)
    #TRANSFORM:
    df = to_pandas(filename, downloaded_csv_path, logger)
    create_table(df, conn, logger)
    format_sales_values(conn, logger)
    #LOAD:    
    load_to_csv(conn, logger)
    
    logger.info("Pipeline ETL concluído com sucesso.")

    conn.close()

if __name__ == "__main__":
    etl_games_sales()
