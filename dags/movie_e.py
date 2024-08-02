from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (PythonOperator, PythonVirtualenvOperator, BranchPythonOperator)
from airflow.models import Variable
from pprint import pprint

with DAG(
    'Movie_E_1234',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='About movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'api', 'extract', 'project'],
) as dag:

    def gen_empty(*ids):
        tasks=[]
        for id in ids:
            task=EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks)

    branch_op=EmptyOperator(
        task_id="branch.op",
        trigger_rule='all_done'
            )

    rm_dir=EmptyOperator(
        task_id='rm.dir',
        trigger_rule="all_done"
            )

    get_data=EmptyOperator(
        task_id='get.data.start',
        trigger_rule='all_done'
            )
    
    save_data=EmptyOperator(
        task_id='save_data',
        trigger_rule='all_done'
            )
   
    start, end=gen_empty('start', 'end')

    start >> branch_op >> rm_dir >> get_data
    branch_op >> get_data

    get_data >> end
