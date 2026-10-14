"""
DAG du pipeline DVF :
ingestion -> transformation -> chargement -> dbt (modèles + tests).
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Chemins dans le conteneur Airflow
PYTHON = "/home/airflow/pipeline-venv/bin/python"
DBT = "/home/airflow/pipeline-venv/bin/dbt"
SRC = "/opt/pipeline/src"
DBT_DIR = "/opt/pipeline/dbt"

parametres_par_defaut = {
    "owner": "seraphin",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dvf_pipeline",
    description="Pipeline DVF : data.gouv -> data lake -> PostgreSQL -> dbt",
    schedule="0 6 1 * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Paris"),
    catchup=False,
    default_args=parametres_par_defaut,
    tags=["dvf", "data-engineering"],
) as dag:

    ingestion = BashOperator(
        task_id="ingestion",
        bash_command=f"{PYTHON} {SRC}/ingestion.py",
    )

    transformation = BashOperator(
        task_id="transformation",
        bash_command=f"{PYTHON} {SRC}/transformation.py",
    )

    chargement = BashOperator(
        task_id="chargement",
        bash_command=f"{PYTHON} {SRC}/chargement.py",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    ingestion >> transformation >> chargement >> dbt_build