from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (PythonOperator, PythonVirtualenvOperator, BranchPythonOperator)

from airflow.models import Variable
from pprint import pprint as pp

with DAG(
    'Movie_T_1234',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
#    max_active_runs= 1,
#    max_active_tasks=3,
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

    task_type=EmptyOperator(
        task_id='type.cast',
        trigger_rule='all_done'
            )

    merge_df=EmptyOperator(
        task_id='merge.df',
        trigger_rule='all_done'
            )

    de_dup=EmptyOperator(
        task_id='de.dup',
        trigger_rule='all_done'
            )

    start, end=gen_empty('start', 'end')

    start >> task_type >> merge_df >> de_dup >> end

