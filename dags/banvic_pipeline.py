from __future__ import annotations
import os
from pathlib import Path
from datetime import timedelta

import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Diretórios
DATALAKE = os.getenv("DATALAKE", "/opt/airflow/extracted_data")
SOURCE_CSV = os.getenv("SOURCE_CSV", "/opt/airflow/data/transacoes.csv")

# Tabelas que vêm do banco fonte
TABLES = ["agencias", "clientes", "colaboradores", "contas", "propostas_credito"]

with DAG(
    dag_id="banvic_etl_daily",
    description="Extrai CSV e tabelas SQL para datalake e carrega no DWH",
    start_date=pendulum.datetime(2025, 8, 25, tz="America/Sao_Paulo"),
    schedule="35 4 * * *",   # todo dia às 04:35
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["banvic", "etl", "datalake", "dwh"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # --- Extração ---
    @task_group(group_id="extract_sources", tooltip="Extrai CSV e tabelas SQL em paralelo")
    def extract_sources():
        @task(task_id="extract_csv")
        def extract_csv(ds: str) -> str:
            """Extrai o CSV local e salva no datalake"""
            dest_dir = Path(DATALAKE) / ds / "csv"
            dest_dir.mkdir(parents=True, exist_ok=True)

            output = dest_dir / "transacoes.csv"
            pd.read_csv(SOURCE_CSV).to_csv(output, index=False)

            return str(output)

        @task(task_id="extract_sql")
        def extract_sql(ds: str) -> list[str]:
            """Extrai tabelas do Postgres (schema banvic)"""
            dest_dir = Path(DATALAKE) / ds / "sql"
            dest_dir.mkdir(parents=True, exist_ok=True)

            hook = PostgresHook(postgres_conn_id="banvic_source_db")
            paths = []
            for table in TABLES:
                df = hook.get_pandas_df(f"SELECT * FROM {table}")
                output = dest_dir / f"{table}.csv"
                df.to_csv(output, index=False)
                paths.append(str(output))

            return paths

        csv_ok = extract_csv(ds="{{ ds }}")
        sql_ok = extract_sql(ds="{{ ds }}")

    # --- Carregamento ---
    @task(task_id="load_dwh")
    def load_dwh(ds: str) -> None:
        """Carrega os CSVs extraídos para o DWH (schema banvic_dw)"""
        base = Path(DATALAKE) / ds
        engine = PostgresHook(postgres_conn_id="banvic_dwh").get_sqlalchemy_engine()

        with engine.begin() as conn:
            # Transações do CSV
            pd.read_csv(base / "csv" / "transacoes.csv").to_sql(
                "transacoes", con=conn, schema="banvic_dw", if_exists="replace", index=False
            )

            # Tabelas SQL extraídas
            for table in TABLES:
                pd.read_csv(base / "sql" / f"{table}.csv").to_sql(
                    table, con=conn, schema="banvic_dw", if_exists="replace", index=False
                )

    # --- Fluxo ---
    start >> extract_sources() >> load_dwh(ds="{{ ds }}") >> end
