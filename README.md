# Video Game Sales Data Analysis 🎮

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline for video game sales data analysis, originally developed as part of a technical assessment for a Data Analyst position.
## 📋 About the Project

The project implements an ETL process using Python, where:

- **Extract**: Automatic data extraction from a CSV file hosted on Google Drive
- **Transform**: Data processing and cleaning
- **Load**: Loading data in two different formats:
  - A DuckDB database for advanced analysis
  - A formatted CSV for use in Power BI

## 🚀 Technologies Used

- Python for data processing
- Prefect for pipeline orchestration
- DuckDB as analytical database
- Jupyter Notebook for exploratory analysis

## 📁 Project Structure

```
├── data
│   ├── BASE_DADOS.csv          # Original dataset
│   ├── db_games_sales.db       # Generated DuckDB database
│   └── games_sales.csv         # Processed dataset for Power BI
├── dependencies
│   ├── Pipfile.lock           # Pipenv dependencies
│   └── requirements.txt       # Pip dependencies
├── etl
│   └── etl.py                # Main ETL script
├── notebook
│   └── analysis.ipynb        # Analysis notebook
```

## 🛠️ How to Run

### Prerequisites

1. Python 3.x installed
2. Package manager pip or Pipenv

### Installation

1. Clone the repository:
```bash
git clone [REPOSITORY_URL]
cd [REPOSITORY_NAME]
```

2. Install dependencies:

With pip:
```bash
pip install -r dependencies/requirements.txt
```

Or with Pipenv:
```bash
pipenv install
```

### Running the Pipeline

1. Start the Prefect server:
```bash
prefect server start
```

2. In another terminal, run the ETL script:
```bash
python etl/etl.py
```

> **Note**: The Prefect server will provide a local URL to access the dashboard with detailed information about pipeline execution. To stop the server, use Ctrl+C in the terminal.

## 📊 Data Analysis

- The analysis process is documented in the [Jupyter notebook](notebook/analysis.ipynb)
- Original data can be accessed through this [Google Drive link](https://drive.google.com/file/d/1eoy8MlYin9PxbCjozT0kjPXPsq0RXEgY/view?usp=drive_link) (no authentication required)

## 🔍 Prefect Features

The project uses Prefect, a modern alternative to Apache Airflow, for:
- Data pipeline orchestration
- Execution monitoring
- Detailed logging
- Visual interface for tracking

## 📝 Additional Notes

- The original dataset is available in the `data` folder as `BASE_DADOS.csv`
- The generated DuckDB database (`db_games_sales.db`) contains processed data ready for analysis
- The `games_sales.csv` file is specifically formatted for use in Power BI
