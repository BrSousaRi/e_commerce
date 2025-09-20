from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Configuração básica
default_args = {
    "owner": "data-team",
    "start_date": datetime(2025, 9, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG
dag = DAG(
    "pipeline_completa_ecommerce",
    default_args=default_args,
    description="Pipeline completa: CSVs → Silver → Gold",
    schedule_interval="0 6 * * *",  # 6:00 da manhã todos os dias
    catchup=False,
)

# Task 1: Início
start = DummyOperator(
    task_id="inicio",
    dag=dag,
)

# Task 2: Upload novos CSVs (SIMPLIFICADO)
upload_csvs = BashOperator(
    task_id="upload_csvs",
    bash_command="cd /opt/airflow && python dags/upload_csv.py",
    dag=dag,
)

# Task 3: Executar Silver
dbt_silver = BashOperator(
    task_id="dbt_silver",
    bash_command="cd /opt/airflow/dbt && dbt run --models silver",
    dag=dag,
)

# Task 4: Executar Gold
dbt_gold = BashOperator(
    task_id="dbt_gold",
    bash_command="cd /opt/airflow/dbt && dbt run --models gold",
    dag=dag,
)

# Task 5: Fim
fim = DummyOperator(
    task_id="fim",
    dag=dag,
)

# Sequência completa das tasks
start >> upload_csvs >> dbt_silver >> dbt_gold >> fim