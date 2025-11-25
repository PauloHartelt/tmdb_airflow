# dags/tmdb_etl_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
import os
import importlib

sys.path.append('/opt/airflow')
import scripts.extract_helpers as helpers

# Configurações principais
RAW_CSV = "/opt/airflow/data_sources/TMDB_movie_dataset_v11.csv"
BRONZE_CSV = "/opt/airflow/outputs/tmdb_bronze.csv"
PROCESSED_DIR = "/opt/airflow/data/processed"

RDS = {
    'host': os.environ.get('RDS_HOST', 'tmdb-database.cbru2wvzl56r.us-east-1.rds.amazonaws.com'),
    'port': int(os.environ.get('RDS_PORT', 3306)),
    'user': os.environ.get('RDS_USER', 'admin'),
    'password': os.environ.get('RDS_PASSWORD', 'equipe1930'),
    'db': os.environ.get('RDS_DB', 'tmdb_analytics')
}

default_args = {
    "owner": "paulo",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------- FUNÇÃO DE LEITURA MAIS RÁPIDA -----------

def fast_read_csv(path):
    return pd.read_csv(
        path, 
        engine="pyarrow",  # 3–5x mais rápido, se disponível
        dtype_backend="pyarrow"
    )

# ---------- DAG -------------------------------------

with DAG(
    dag_id="tmdb_etl_pipeline_fast",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tmdb", "etl", "optimized"],
) as dag:

    def task_extract():
        df = fast_read_csv(RAW_CSV)
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        df.to_csv(os.path.join(PROCESSED_DIR, "raw_copy.csv"), index=False)
        return True

    def task_bronze():
        df = fast_read_csv(RAW_CSV)
        df_clean = helpers.basic_clean(df)
        helpers.save_bronze(df_clean, BRONZE_CSV)
        return True

    def task_transform_and_load():
        importlib.reload(helpers)

        # 🔥 LEITURA SUPER RÁPIDA DO BRONZE
        df = fast_read_csv(BRONZE_CSV)

        # 🔥 CRIA MOTOR COM MELHOR POOLING
        engine = helpers.create_mysql_engine(
            RDS['host'], RDS['port'],
            RDS['user'], RDS['password'], RDS['db'],
            pool_size=10,
            max_overflow=20,
        )

        # ==============================================
        #  DIM-IDIOMA
        # ==============================================
        if "original_language" in df.columns:

            langs = (
                df[["original_language"]]
                .dropna()
                .drop_duplicates()
                .reset_index(drop=True)
            )
            langs["id_idioma"] = langs.index + 1
            langs.rename(columns={"original_language": "nome_idioma"}, inplace=True)

            helpers.write_table_engine(
                langs[["id_idioma", "nome_idioma"]],
                "Dim_Idioma",
                engine,
                if_exists="replace",
                chunksize=50_000,
                method="multi"
            )

            lang_map = dict(zip(langs["nome_idioma"], langs["id_idioma"]))

        # # ==============================================
        # #  DIM-FILME
        # # ==============================================
        filme_cols = ['id', 'title', 'original_title', 'status', 'overview',
                      'homepage', 'imdb_id', 'tagline', 'backdrop_path',
                      'poster_path', 'original_language']

        filme_cols = [c for c in filme_cols if c in df.columns]

        dim_filme = (
            df[filme_cols]
            .drop_duplicates(subset=["id"])
            .rename(columns={"id": "id_filme"})
        )

        if 'original_language' in dim_filme.columns:
            dim_filme["id_idioma_original"] = dim_filme["original_language"].map(lang_map)

        helpers.write_table_engine(
            dim_filme.drop(columns=["original_language"], errors="ignore"),
            "Dim_Filme",
            engine,
            if_exists="replace",
            chunksize=50_000,
            method="multi"
        )

        # ==============================================
        #  DIM TEMPO + FATO FILME
        # ==============================================
        if "release_date" in df.columns:

             # 🔥 transforma sem criar vários novos DataFrames
             tempo = df[["id", "release_date"]].dropna().drop_duplicates()
             tempo["release_date"] = pd.to_datetime(tempo["release_date"], errors="coerce")
             tempo = tempo.dropna(subset=["release_date"])

             tempo["ano"] = tempo["release_date"].dt.year
             tempo["mes"] = tempo["release_date"].dt.month
             tempo["trimestre"] = tempo["release_date"].dt.quarter
             tempo["decada"] = (tempo["ano"] // 10) * 10
             tempo.rename(columns={"id": "id_filme"}, inplace=True)
             tempo["id_tempo"] = tempo.index + 1

             helpers.write_table_engine(
                 tempo[["id_tempo", "release_date", "ano", "mes", "trimestre", "decada"]],
                 "Dim_Tempo",
                 engine,
                 if_exists="replace",
                 chunksize=50_000,
                 method="multi"
             )

        tempo_map = dict(zip(tempo["id_filme"], tempo["id_tempo"]))

        fato = (
            df[["id", "budget", "revenue", "popularity", "vote_average",
                "vote_count", "runtime", "adult"]]
            .drop_duplicates(subset=["id"])
            .rename(columns={"id": "id_filme"})
        )
        fato["id_tempo"] = fato["id_filme"].map(tempo_map)

        helpers.write_table_engine(
            fato,
            "Fato_Filme",
            engine,
            if_exists="replace",
            chunksize=50_000,
            method="multi"
        )

        # ==============================================
        #  GÊNEROS (otimizado)
        # ==============================================
        if "genres" in df.columns:
            gen_df = helpers.explode_json_column(df, "genres", id_col="id")
            if gen_df is not None and not gen_df.empty:
                gen_df.rename(columns={"film_id": "id_filme"}, inplace=True)
                gen_df["id_genero"] = gen_df["item_id"].fillna(
                    gen_df["item_name"].apply(helpers.name_to_int_id)
                )
                dim_gen = gen_df[["id_genero", "item_name"]].drop_duplicates()
                dim_gen.rename(columns={"item_name": "nome_genero"}, inplace=True)

                helpers.write_table_engine(dim_gen, "Dim_Genero", engine,
                                           if_exists="replace", chunksize=50_000,
                                           method="multi")

                filme_gen = gen_df[["id_filme", "id_genero"]].dropna()
                helpers.write_table_engine(filme_gen, "Filme_Genero", engine,
                                           if_exists="replace", chunksize=50_000,
                                           method="multi")

        # ==============================================
        #  DIM COMPANHIAS
        # ==============================================
        if "production_companies" in df.columns:
            comp_df = helpers.explode_json_column(df, "production_companies", id_col="id")

            if comp_df is not None and not comp_df.empty:

                comp_df.rename(columns={"film_id": "id_filme"}, inplace=True)

                comp_df["id_companhia"] = comp_df["item_id"].fillna(
                    comp_df["item_name"].apply(helpers.name_to_int_id)
                )

                dim_comp = (
                    comp_df[["id_companhia", "item_name"]]
                    .drop_duplicates()
                    .rename(columns={"item_name": "nome_companhia"})
                )

                helpers.write_table_engine(
                    dim_comp,
                    "Dim_Companhia",
                    engine,
                    if_exists="replace",
                    chunksize=50_000,
                    method="multi"
                )

                filme_comp = comp_df[["id_filme", "id_companhia"]].dropna()

                helpers.write_table_engine(
                    filme_comp,
                    "Filme_Companhia",
                    engine,
                    if_exists="replace",
                    chunksize=50_000,
                    method="multi"
                )

        # ==============================================
        #  DIM PAÍSES
        # ==============================================
        if "production_countries" in df.columns:
            country_df = helpers.explode_json_column(df, "production_countries", id_col="id")

            if country_df is not None and not country_df.empty:

                country_df.rename(columns={"film_id": "id_filme"}, inplace=True)

                country_df["id_pais"] = country_df["item_id"].fillna(
                    country_df["item_name"].apply(helpers.name_to_int_id)
                )

                dim_country = (
                    country_df[["id_pais", "item_name"]]
                    .drop_duplicates()
                    .rename(columns={"item_name": "nome_pais"})
                )

                helpers.write_table_engine(
                    dim_country,
                    "Dim_Pais",
                    engine,
                    if_exists="replace",
                    chunksize=50_000,
                    method="multi"
                )

                filme_country = country_df[["id_filme", "id_pais"]].dropna()

                helpers.write_table_engine(
                    filme_country,
                    "Filme_Pais",
                    engine,
                    if_exists="replace",
                    chunksize=50_000,
                    method="multi"
                )

        # ==============================================
        #  DIM KEYWORDS
        # ==============================================
        if "keywords" in df.columns:
            kw_df = helpers.explode_json_column(df, "keywords", id_col="id")

            if kw_df is not None and not kw_df.empty:

                kw_df.rename(columns={"film_id": "id_filme"}, inplace=True)

                kw_df["id_keyword"] = kw_df["item_id"].fillna(
                    kw_df["item_name"].apply(helpers.name_to_int_id)
                )

                dim_kw = (
                    kw_df[["id_keyword", "item_name"]]
                    .drop_duplicates()
                    .rename(columns={"item_name": "palavra_chave"})
                )

                helpers.write_table_engine(
                    dim_kw,
                    "Dim_Keyword",
                    engine,
                    if_exists="replace",
                    chunksize=50_000,
                    method="multi"
                )

                filme_kw = kw_df[["id_filme", "id_keyword"]].dropna()

                helpers.write_table_engine(
                    filme_kw,
                    "Filme_Keyword",
                    engine,
                    if_exists="replace",
                    chunksize=50_000,
                    method="multi"
                )

        return True

    t1 = PythonOperator(task_id="extract_raw", python_callable=task_extract)
    t2 = PythonOperator(task_id="create_bronze", python_callable=task_bronze)
    t3 = PythonOperator(task_id="transform_and_load", python_callable=task_transform_and_load)

    t1 >> t2 >> t3