from pathlib import Path
import pandas as pd
import duckdb
import wget
from prefect import flow, task
from prefect.logging import get_run_logger

@task(retries=3, retry_delay_seconds=5)
def download_csv(filename: str, save_directory: str, downloaded_csv_path: Path, logger: get_run_logger) -> None:
    """
    Downloads a CSV file from a specific URL and saves it locally.

    Args:
        filename (str): Name of the CSV file to be downloaded.
        save_directory (str): Directory where the file will be saved.
        downloaded_csv_path (Path): Complete path for the downloaded CSV file.
        logger (get_run_logger): Logger to record process information.

    Raises:
        URLError: If an error occurs during file download.
    """
    logger.info(f"Starting download of {filename}...")
    download_url = 'https://drive.google.com/uc?export=download&id=1eoy8MlYin9PxbCjozT0kjPXPsq0RXEgY'
    logger.info(f"Downloading {filename} from {download_url} to {save_directory}")
    wget.download(download_url, out=str(downloaded_csv_path))
    logger.info(f"Download complete.")

@task
def to_pandas(filename: str, downloaded_csv_path: Path, logger: get_run_logger) -> pd.DataFrame:
    """
    Reads a CSV file and converts it to a pandas DataFrame.

    Args:
        filename (str): Name of the CSV file to be read.
        downloaded_csv_path (Path): Path to the CSV file.
        logger (get_run_logger): Logger to record process information.

    Returns:
        pd.DataFrame: DataFrame containing the CSV file data.
    """
    logger.info(f"Reading {filename} into a pandas DataFrame...")
    df = pd.read_csv(downloaded_csv_path, sep=';', encoding='latin-1').reset_index().rename(columns={'index': 'index'})
    logger.info(f"File {filename} successfully read. Shape: {df.shape}")
    return df

@task
def create_table(df: pd.DataFrame, conn: duckdb.DuckDBPyConnection, logger: get_run_logger) -> None:
    """
    Creates the 'games_sales' table in DuckDB from a DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the data to be inserted into the table.
        conn (duckdb.DuckDBPyConnection): Connection to the DuckDB database.
        logger (get_run_logger): Logger to record process information.
    """
    logger.info("Creating 'games_sales' table in DuckDB...")
    conn.sql("DROP TABLE IF EXISTS games_sales;")
    query = """
    CREATE TABLE games_sales AS
    SELECT 
        *,
        (Sales_NA + Sales_EU + Sales_JP + Sales_Others) AS Global_Sales
    FROM df;
    """
    conn.sql(query)
    logger.info("Table 'games_sales' created successfully.")

@task
def format_sales_values(conn: duckdb.DuckDBPyConnection, logger: get_run_logger) -> None:
    """
    Formats the sales values in the 'games_sales' table to DOUBLE type and divides by 1000.

    Args:
        conn (duckdb.DuckDBPyConnection): Connection to the DuckDB database.
        logger (get_run_logger): Logger to record process information.
    """
    logger.info("Formatting sales values in 'games_sales' table...")
    columns = ["Sales_NA", "Sales_EU", "Sales_JP", "Sales_Others", "Global_Sales"]
    alter_sql = "ALTER TABLE games_sales ALTER COLUMN $column_name SET DATA TYPE DOUBLE USING $column_name / 1000.0;"
    for column in columns:
        conn.sql(alter_sql.replace("$column_name", column))
    logger.info("Sales values formatted successfully.")

@task
def load_to_csv(conn: duckdb.DuckDBPyConnection, logger: get_run_logger) -> None:
    """
    Exports the data from 'games_sales' table to a CSV file.

    Args:
        conn (duckdb.DuckDBPyConnection): Connection to the DuckDB database.
        logger (get_run_logger): Logger to record process information.
    """
    logger.info("Exporting data from 'games_sales' table to CSV file...")
    conn.sql("""
    COPY games_sales TO 'data/games_sales.csv' (HEADER, DELIMITER ';')
    """)
    logger.info("Data exported successfully.")

@flow
def etl_games_sales() -> None:
    """
    Executes the ETL (Extract, Transform, Load) flow to process game sales data.

    1. Downloads the CSV file containing sales data.
    2. Reads the CSV into a pandas DataFrame.
    3. Creates a 'games_sales' table in DuckDB from the DataFrame.
    4. Formats the sales values in the table to DOUBLE type and divides by 1000.
    5. Exports the data from 'games_sales' table to a CSV file.

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
    logger.info("Starting ETL pipeline...")
    
    # EXTRACT
    download_csv(filename, save_directory, downloaded_csv_path, logger)
    
    # TRANSFORM
    df = to_pandas(filename, downloaded_csv_path, logger)
    create_table(df, conn, logger)
    format_sales_values(conn, logger)
    
    # LOAD
    load_to_csv(conn, logger)
    
    logger.info("ETL pipeline completed successfully.")
    conn.close()

if __name__ == "__main__":
    etl_games_sales()
