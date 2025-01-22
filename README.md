# Video Game Sales Analysis
> A data analysis project exploring video game sales data to evaluate potential gaming partnerships for a delivery platform.

## 📊 About
A solution developed for a Data Analyst technical challenge that processes and analyzes historical video game sales data. The project consists of an ETL pipeline that:
- Extracts data from a CSV file hosted on Google Drive
- Processes and cleans the data
- Loads the results into:
  - A DuckDB database for analysis
  - A formatted CSV file for Power BI visualizations

The project uses Prefect for pipeline orchestration, providing:
- Real-time execution monitoring
- Detailed logging system
- Visual dashboard for tracking
- Automated workflow management

### 📝 Analysis Performed
The project answers the following business questions using SQL in Jupyter Notebook:
1. Top 3 best-selling games in 2015
2. Average sales volume for Xbox One in 2016
3. Market share of Sports games compared to other genres since 2000
4. Best-selling game in Japan during 1998
5. **Additional Analysis**: Regional sales distribution (as percentages) for the top 25 games by global sales

## 🛠️ Tech Stack
- Python for data processing
- DuckDB for data storage and SQL queries
- Prefect for pipeline orchestration
- Jupyter Notebook for analysis

## 🗂️ Project Structure
```
├── README.md                   # Project documentation
├── technical_challenge_proposal.md # Original challenge proposal
├── requirements.txt           # Python dependencies
├── data
│   ├── BASE_DADOS.csv          # Original dataset
│   ├── db_games_sales.db       # DuckDB database with processed data
│   └── games_sales.csv         # Processed dataset optimized for Power BI
├── etl
│   └── etl.py                # ETL script
├── notebook
│   └── analysis.ipynb        # Analysis notebook
```

## 🚀 How to Run

### Prerequisites
1. Python 3.x
2. pip

### Installation
1. Clone the repository:
```bash
git clone https://github.com/victor-antoniassi/junior_data_analyst_test_01
cd junior_data_analyst_test_01
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Pipeline
1. Start the Prefect server:
```bash
prefect server start
```

> **Note**: After starting the server, you'll receive a local URL to access Prefect's dashboard where you can monitor the pipeline execution in real-time.

2. In another terminal, run the ETL:
```bash
python etl/etl.py
```

To stop the Prefect server, use Ctrl+C in the terminal.

## 📊 Data Sources
- Original dataset available on [Google Drive](https://drive.google.com/file/d/1eoy8MlYin9PxbCjozT0kjPXPsq0RXEgY/view?usp=drive_link) (no authentication required)
- All SQL queries and analysis are documented in the [analysis notebook](notebook/analysis.ipynb)

---
