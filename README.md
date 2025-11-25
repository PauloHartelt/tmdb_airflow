# Projeto ETL TMDB – Data Warehouse com Airflow

## 📌 Descrição

Este projeto implementa um pipeline completo de **ETL (Extract, Transform, Load)** utilizando:

- **Apache Airflow** (via Docker) como orquestrador;
- **MySQL** como banco de dados analítico (Data Warehouse);
- **Docker Compose** para orquestração dos serviços;
- **Python + Pandas** para tratamento dos dados;
- Arquitetura **Bronze → Silver → Gold** inspirada em Data Lakehouse.

O objetivo é processar dados do **The Movie Database (TMDB)**, armazenados inicialmente em arquivo CSV, transformar esses dados em tabelas dimensionais e fato e carregá-los em um **Data Warehouse relacional**.

---

## 🔄 Pipeline ETL

### 🔹 **1. Extração (Bronze Layer)**  
- Lê o arquivo CSV original (`tmdb_raw.csv`).
- Realiza limpeza leve (datas, tipos numéricos, booleanos).
- Salva um arquivo Bronze padronizado (`tmdb_bronze.csv`).

### 🔹 **2. Transformação (Silver Layer)**  
- Enriquecimento dos dados.
- Geração de dimensões:
  - `Dim_Filme`, `Dim_Tempo`, `Dim_Idioma`, `Dim_Genero`, `Dim_Companhia`, `Dim_Pais`, `Dim_Keyword`
- Geração das tabelas fato e associativas:
  - `Fato_Filme`, `Filme_Genero`, `Filme_Companhia`, `Filme_Pais`, `Filme_Keyword`

### 🔹 **3. Carga (Gold Layer / DWH)**  
- Inserção em banco MySQL via SQLAlchemy.
- Dimensões persistentes.
- Fatos com append incremental.

---

## 🛠️ Serviços Docker Utilizados

- Airflow Webserver — porta 8080  
- Airflow Scheduler  
- Airflow DB  
- PostgreSQL DWH  
- Volumes persistentes de logs e banco  

Definidos em `docker-compose.yml`.

---

## ▶️ Como executar o projeto

### **1. Instalar dependências**
```bash
pip install -r requirements.txt
```

### **2. Iniciar Docker Desktop**

### **3. Subir containers**
```bash
docker compose up
```

### **4. Acessar o Airflow**
```
http://localhost:8080/login/
```
Login:
- admin / admin

### **5. Executar a DAG**
Menu → tmdb_etl_pipeline_fast → Trigger DAG

---

## 🗂 Estrutura do Projeto

```bash
tmdb-etl-project/
├── airflow/
│   ├── dags/
│   │   └── tmdb_dag.py
│   ├── logs/...
│   ├── plugins/
│   └── scripts/
│       ├── __init__.py
│       ├── extract_helpers.py
│       └── sql_schema.sql
├── data_sources/
│   ├── TMDB_movie_dataset_v11.csv
│   └── outputs/
├── airflow-db-data/...
├── docker-compose.yml
├── requirements.txt
├── Projeto Eixo 4.pbix
└── README.md
```
---

## 🎯 Objetivos atendidos

- Pipeline real com Airflow + Docker + RDS
- Camadas Bronze/Silver/Gold  
- Modelo dimensional completo  
- Carga incremental  
- Execução diária  
- Logging estruturado
- Dados sendo consumidos no Power BI  
