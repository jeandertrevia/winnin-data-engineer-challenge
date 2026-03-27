import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email

ADMIN_EMAIL = os.getenv("AIRFLOW_ADMIN_EMAIL")
DBT_DIR = "/opt/airflow/dbt"


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
    dag_id="dag_dbt",
    description="Executa os modelos DBT apos a extracao (staging, intermediate, mart)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "transform"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    dbt_run >> dbt_test
