from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
# from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
    dag_id = 'Uniube',
    tags=["RTO"],
    default_args = default_args,
    start_date = datetime(2022, 1, 12),
    #retry_delay = timedelta(minutes=24),
    schedule_interval = '0 6,12,16,20 * * *',
    catchup = False
) as dag:

    Dimensions = BashOperator(
        task_id='Dimensoes',
        bash_command = 'python C:\Projects\python\ETLProject-1\src\Dimensions.py',
        dag = dag
    )
    Facts = BashOperator(
        task_id='DimensoesProcessamento',
        bash_command = 'python src/Facts.py',
        dag = dag
    )


    Dimensions >> Facts