import sys
import os
from datetime import timedelta

import pendulum
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag,task

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt"
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_ENV = {"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs"}
RETRY_DELAY = timedelta(seconds=int(os.getenv("RETRY_DELAY_SEC", "300")))

def avisar_falha(context):
    ti = context['task_instance']
    texto = (
        ':rotating_light: *Falha no pipeline BCB*\n'
        f'DAG: {ti.dag_id} | Task: {ti.task_id}\n'
        f"Execução: {context.get('run_id')}\n"
        f"Erro: {str(context.get('exception'))[:500]}"
    )
    url = os.getenv('ALERT_WEBHOOK_URL')
    if not url:
        print(texto)
        return
    resp = requests.post(url, json={'text': texto}, timeout=10)
    if not resp.ok:
        print(f'Alerta não enviado: HTTP {resp.status_code}')
        
default_args = {
    'retries': 3,
    'retry_delay': RETRY_DELAY,
    'on_failure_callback': avisar_falha
}

@dag(
    dag_id="pipeline_bcb_diario",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args=default_args,
    tags=["bcb", "d2"]
)
def pipeline_bcb_diario():
    @task(execution_timeout=timedelta(minutes=10))
    def extrair_e_carregar():
        sys.path.insert(0, f"{PROJECT_DIR}/extract")
        import load_bq
        
        load_bq.main()
        
    dbt_run = BashOperator(
        task_id= "dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} seed && {DBT_BIN} run",
        env=DBT_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=5)
    )
    
    dbt_test = BashOperator(
        task_id = "dbt_test",
        bash_command= f"cd {DBT_DIR} && {DBT_BIN} test",
        env= DBT_ENV,
        append_env= True,
        retries=0,
        execution_timeout=timedelta(minutes=15)
    )
    
    extrair_e_carregar() >> dbt_run >> dbt_test

pipeline_bcb_diario()