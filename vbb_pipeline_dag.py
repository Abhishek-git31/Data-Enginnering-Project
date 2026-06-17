from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'vbb_team',
    'start_date': datetime(2026, 5, 1),
    'retries': 0,
}

with DAG(
    dag_id='vbb_transit_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['vbb', 'data-engineering'],
) as dag:

    t1 = BashOperator(
        task_id='run_vbb_pipeline',
        bash_command='/opt/anaconda3/bin/python "/Users/abhishekkarthikakunuru/Desktop/Data Engineering /DAG_pipeline.py"',
    )

    t2 = EmptyOperator(task_id='pipeline_complete')

    t1 >> t2