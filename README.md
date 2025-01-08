# Video Game Sales Analysis
Solution built for a Data Analyst technical assessment. Read the complete challenge proposal [here](technical_challenge_proposal.md).

## 📊 About
Solution developed for a technical assessment that analyzed video game sales data to support gaming partnership decisions. The project demonstrates a complete ETL (Extract, Transform, Load) pipeline for video game sales data analysis, where:
- **Extract**: Automatic data extraction from a CSV file hosted on Google Drive
- **Transform**: Data processing and cleaning
- **Load**: Loading data in two different formats:
  - A DuckDB database for advanced analysis
  - A formatted CSV for use in Power BI

## 🛠️ Technical Stack
- Python for data processing
- Prefect for pipeline orchestration
- DuckDB as analytical database
- Jupyter Notebook for exploratory analysis

## 🗂️ Project Structure
```
├── README.md                   # Project documentation
├── technical_challenge_proposal.md # Original challenge details
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

## 🔧 How to Run
### Prerequisites
1. Python 3.x installed
2. Package manager pip or Pipenv

### Installation
1. Clone the repository:
```bash
git clone https://github.com/victor-antoniassi/junior_data_analyst_test_01.git
cd junior_data_analyst_test_01
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

---
*Note: This project was developed as part of a technical assessment for a Data Analyst position. Some implementation details go beyond the original requirements to demonstrate technical capabilities.*
