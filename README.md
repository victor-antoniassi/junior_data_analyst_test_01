# Case/teste de analista de dados
Simulei um processo simples de ETL, através de um [script](etl/etl.py) em Python, que extrai o [arquivo csv original](data/BASE_DADOS.csv), através de um [Google Drive](https://drive.google.com/file/d/1eoy8MlYin9PxbCjozT0kjPXPsq0RXEgY/view?usp=drive_link) (link público, não é necessário autenticação), faz pequenas transformação para ajustar os dados e os carrega em outros dois arquivos finais: Um [banco de dados DuckDB](data/db_games_sales.db) que foi utilizado na etapa de análise em um [jupyter notebook](notebook/analysis.ipynb) e um [arquivo csv](data/games_sales.csv) para uso no Power BI.

```
├── data
│   ├── BASE_DADOS.csv
│   ├── db_games_sales.db
│   └── games_sales.csv
├── dependencies
│   ├── Pipfile.lock
│   └── requirements.txt
├── etl
│   └── etl.py
├── notebook
│   └── analysis.ipynb
```

### Dependências para execução do script/notebook
Dentro do diretório dependencies se encontra os arquivos que listam os pacotes/libs necessárias para execução correta do script/notebook, [requirements.txt](dependencies/requirements.txt) pode ser utlizado na maioria dos gerenciadores de dependência/ambientes virtuais e o [Pipfile.lock](dependencies/Pipfile.lock) é utilizado apenas pelo gerenciador Pipenv.
Após instalado as dependencias e ativado o ambiente virtual, é necessário antes de executar o .py, executar o comando ```prefect server start``` no terminal que você utiliza no seu sistema operacional,  esse comando ativa o Prefect Server de forma local, utilizei de forma bem simples a ferramenta/lib [Prefect](https://docs-3.prefect.io/3.0/get-started/quickstart) (ferramenta similar ao Apache Airflow), para orquestrar a execução do processo de ETL, utilizei principalmente para o uso dos recursos úteis de logs que o Prefect oferece, é possível acessar o dashboard que exibe mais informações da execução do script acessando a url localhost fornecida ao executar o servidor do Prefect. Para desligar o server, é só utilizar o comando control+c ou fechar a tela do terminal.
