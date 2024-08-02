from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (PythonOperator, PythonVirtualenvOperator, BranchPythonOperator)

from airflow.models import Variable
from pprint import pprint as pp

with DAG(
    'Movie_L_1234',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='About movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'api', 'amt'],
) as dag:
    def gen_empty(*ids):
        tasks=[]
        for id in ids:
            task=EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks)

    task_summary=EmptyOperator(
        task_id='task.summary',
        trigger_rule='all_done'
            )


    start, end=gen_empty('start', 'end')

    start >> task_summary >> end
