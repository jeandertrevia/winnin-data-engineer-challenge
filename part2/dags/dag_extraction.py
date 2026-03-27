import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.email import send_email

sys.path.insert(0, "/opt/airflow/extraction")
from extract_and_load import run as run_extraction

ADMIN_EMAIL = os.getenv("AIRFLOW_ADMIN_EMAIL", "jeandercarneiro@gmail.com")


def on_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    exception = context.get("exception")

    send_email(
        to=ADMIN_EMAIL,
        subject=f"[Airflow] ERRO em {dag_id}.{task_id}",
        html_content=f"""
            <h3>Falha no pipeline</h3>
            <b>DAG:</b> {dag_id}<br>
            <b>Task:</b> {task_id}<br>
            <b>Erro:</b> {exception}<br>
            <b>Log:</b> <a href="{log_url}">{log_url}</a>
        """,
    )


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure,
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_extraction",
    description="Extrai dados da YouTube API e carrega no Postgres raw layer",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["extraction", "youtube", "raw"],
) as dag:

    extract_and_load = PythonOperator(
        task_id="extract_and_load",
        python_callable=run_extraction,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dag_dbt",
        wait_for_completion=False,
    )

    extract_and_load >> trigger_dbt
