from airflow import DAG
import os
import sys
from airflow.operators.python import PythonOperator
import datetime
import subprocess
import getpass

subprocess.call([sys.executable, "-m", "pip", "install", "pymongo"])
sys.path.insert(0, os.path.abspath(os.path.join(
    os.path.dirname(__file__), '.', 'scripts')))

from labelled_authors_to_final_db import labelled_authors_to_final_db
from update_labelled_authors import update_labelled_authors
from merge_authors import merge_authors
from political_leaning_query import political_leaning_query
from personality_query import personality_query
from nationality_query import nationality_query
from age_gender_query import age_gender_query
from create_main_collection_indices import create_main_collection_indices
from get_remove_database_month import get_remove_database_month

default_args = {
    'owner': 'chrisemmery',
    'depends_on_past': False,
    'email': ['s.kramp@tilburguniversity.edu'],
    'start_date': datetime.datetime(2023, 5, 2),
    'email_on_failure': True
}

dag = DAG(
    'mine_reddit_month',
    default_args=default_args,
    description='A pipeline that queries data from MongoDB and stores the results in a new collection',
    max_active_runs=1,
    max_active_tasks=10,
    schedule_interval=None
)


def run_get_remove_database_month(ds=None, **kwargs):
    get_remove_database_month(get_database_month=kwargs['dag_run'].conf.get('get_database_month'),
                              remove_database_month=kwargs['dag_run'].conf.get('remove_database_month'),
                              no_submissions=kwargs['dag_run'].conf.get('no_submissions', False),
                              no_comments=kwargs['dag_run'].conf.get('no_comments', False))


def run_create_main_collection_indices(ds=None, **kwargs):
    create_main_collection_indices(kwargs['dag_run'].conf.get('query_month'))


def run_age_gender_query(ds=None, **kwargs):
    age_gender_query(kwargs['dag_run'].conf.get('query_month'))


def run_nationality_query(ds=None, **kwargs):
    nationality_query(kwargs['dag_run'].conf.get('query_month'))


def run_personality_query(ds=None, **kwargs):
    personality_query(kwargs['dag_run'].conf.get('query_month'))


def run_political_leaning_query(ds=None, **kwargs):
    political_leaning_query(kwargs['dag_run'].conf.get('query_month'))


def run_update_labelled_authors(ds=None, **kwargs):
    update_labelled_authors()


def run_labelled_authors_to_final_db(ds=None, **kwargs):
    labelled_authors_to_final_db(kwargs['dag_run'].conf.get('query_month'))


get_remove_database_month_task = PythonOperator(
    task_id=run_get_remove_database_month.__name__,
    python_callable=run_get_remove_database_month,
    dag=dag,
)

# create_main_collection_indices_task = PythonOperator(
#     task_id=run_create_main_collection_indices.__name__,
#     python_callable=run_create_main_collection_indices,
#     dag=dag,
# )

query_age_gender_task = PythonOperator(
    task_id=run_age_gender_query.__name__,
    python_callable=run_age_gender_query,
    dag=dag,
)

query_personality_task = PythonOperator(
    task_id=run_personality_query.__name__,
    python_callable=run_personality_query,
    dag=dag,
)

query_nationality_task = PythonOperator(
    task_id=run_nationality_query.__name__,
    python_callable=run_nationality_query,
    dag=dag,
)

query_political_leaning_task = PythonOperator(
    task_id=run_political_leaning_query.__name__,
    python_callable=run_political_leaning_query,
    dag=dag,
)

merge_authors_task = PythonOperator(
    task_id=merge_authors.__name__,
    python_callable=merge_authors,
    dag=dag,
)

update_labelled_authors_task = PythonOperator(
    task_id=run_update_labelled_authors.__name__,
    python_callable=run_update_labelled_authors,
    dag=dag,
)

labelled_authors_to_final_db_task = PythonOperator(
    task_id=run_labelled_authors_to_final_db.__name__,
    python_callable=run_labelled_authors_to_final_db,
    dag=dag,
)

get_remove_database_month_task >>\
    [query_age_gender_task, query_nationality_task, query_personality_task, query_political_leaning_task] >>\
    merge_authors_task >>\
    update_labelled_authors_task >> labelled_authors_to_final_db_task
