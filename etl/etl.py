from pathlib import Path
import pandas as pd
import duckdb
import wget
from prefect import flow, task
from prefect.logging import get_run_logger

@task(retries=3, retry_delay_seconds=5)
def download_csv(filename: str, save_directory: str, downloaded_csv_path: Path, logger: get_run_logger) -> None:
    """
    Baixa um arquivo CSV de uma URL específica e o salva localmente.

    Args:
        filename (str): Nome do arquivo CSV a ser baixado.
        save_directory (str): Diretório onde o arquivo será salvo.
        downloaded_csv_path (Path): Caminho completo para o arquivo CSV baixado.
        logger (get_run_logger): Logger para registrar informações sobre o processo.

    Raises:
        URLError: Se ocorrer um erro durante o download do arquivo.
    """
    logger.info(f"Iniciando download de {filename}...")
    download_url = 'https://drive.google.com/uc?export=download&id=1eoy8MlYin9PxbCjozT0kjPXPsq0RXEgY'
    logger.info(f"Downloading {filename} from {download_url} to {save_directory}")
    wget.download(download_url, out=str(downloaded_csv_path))
    logger.info(f"Download completo.")

@task
def to_pandas(filename: str, downloaded_csv_path: Path, logger: get_run_logger) -> pd.DataFrame:
    """
    Lê um arquivo CSV e o converte em um DataFrame pandas.

    Args:
        filename (str): Nome do arquivo CSV a ser lido.
        downloaded_csv_path (Path): Caminho para o arquivo CSV.
        logger (get_run_logger): Logger para registrar informações sobre o processo.

    Returns:
        pd.DataFrame: DataFrame contendo os dados do arquivo CSV.
    """
    logger.info(f"Lendo {filename} em um DataFrame pandas...")
    df = pd.read_csv(downloaded_csv_path, sep=';', encoding='latin-1').reset_index().rename(columns={'index': 'index'})
    logger.info(f"Arquivo {filename} lido com sucesso. Shape: {df.shape}")
    return df

@task
def create_table(df: pd.DataFrame, conn: duckdb.DuckDBPyConnection, logger: get_run_logger) -> None:
    """
    Cria a tabela 'games_sales' no DuckDB a partir de um DataFrame.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos na tabela.
        conn (duckdb.DuckDBPyConnection): Conexão com o banco de dados DuckDB.
        logger (get_run_logger): Logger para registrar informações sobre o processo.
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
def format_sales_values(conn: duckdb.DuckDBPyConnection, logger: get_run_logger) -> None:
    """
    Formata os valores de vendas na tabela 'games_sales' para o tipo DOUBLE e divide por 1000.

    Args:
        conn (duckdb.DuckDBPyConnection): Conexão com o banco de dados DuckDB.
        logger (get_run_logger): Logger para registrar informações sobre o processo.
    """
    logger.info("Formatando valores de vendas na tabela 'games_sales'...")
    columns = ["Sales_NA", "Sales_EU", "Sales_JP", "Sales_Others", "Global_Sales"]
    alter_sql = "ALTER TABLE games_sales ALTER COLUMN $column_name SET DATA TYPE DOUBLE USING $column_name / 1000.0;"
    for column in columns:
        conn.sql(alter_sql.replace("$column_name", column))
    logger.info("Valores de vendas formatados com sucesso.")

@task
def load_to_csv(conn: duckdb.DuckDBPyConnection, logger: get_run_logger) -> None:
    """
    Exporta os dados da tabela 'games_sales' para um arquivo CSV.

    Args:
        conn (duckdb.DuckDBPyConnection): Conexão com o banco de dados DuckDB.
        logger (get_run_logger): Logger para registrar informações sobre o processo.
    """
    logger.info("Exportando dados da tabela 'games_sales' para um arquivo CSV...")
    conn.sql("""
    COPY games_sales TO 'data/games_sales.csv' (HEADER, DELIMITER ';')
    """)
    logger.info("Dados exportados com sucesso.")

@flow
def etl_games_sales() -> None:
    """
    Executa o fluxo de ETL (Extract, Transform, Load) para processar dados de vendas de jogos.

    1. Baixa o arquivo CSV contendo os dados de vendas.
    2. Lê o CSV em um DataFrame pandas.
    3. Cria uma tabela 'games_sales' no DuckDB a partir do DataFrame.
    4. Formata os valores de vendas na tabela para o tipo DOUBLE e divide por 1000.
    5. Exporta os dados da tabela 'games_sales' para um arquivo CSV.

    Args:
        None

    Returns:
        None
    """
    logger = get_run_logger()
    save_directory = 'data'
    filename = 'BASE_DADOS.csv'
    downloaded_csv_path = Path(f'{save_directory}/{filename}')
    duckdb_path = Path('data/db_games_sales.db')
    conn: duckdb.DuckDBPyConnection = duckdb.connect(duckdb_path)
    logger.info("Iniciando pipeline ETL...")
    
    # EXTRACT
    download_csv(filename, save_directory, downloaded_csv_path, logger)
    
    # TRANSFORM
    df = to_pandas(filename, downloaded_csv_path, logger)
    create_table(df, conn, logger)
    format_sales_values(conn, logger)
    
    # LOAD
    load_to_csv(conn, logger)
    
    logger.info("Pipeline ETL concluído com sucesso.")
    conn.close()

if __name__ == "__main__":
    etl_games_sales()