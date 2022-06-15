import os
import shutil
import json
import pendulum
import requests
from requests.auth import HTTPBasicAuth
import jsonlines
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['tmlangeni@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


http = HttpHook.get_connection('03_shonataxi_http')
BASE_URL = requests.utils.unquote(http.get_uri()[7:])
CUR_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.abspath(os.path.dirname(__file__)) + '/data'

def fnc_initalize_environment(**kwargs):
    ti = kwargs['ti']
    pg_hook = PostgresHook(postgre_conn_id="cairo_pg_conn")
    connection = pg_hook.get_conn()
    cur = connection.cursor()
    sql_file = open(CUR_DIR+'/sql/get_settings.sql', 'r')
    cur.execute(sql_file.read())

    row = cur.fetchone()

    while row is not None:
        partner = '{id}_{partner}'.format(
            id=str(row[0]).zfill(2), partner=row[1])
        s3_dir = partner + '/'+row[2]
        ti.xcom_push(key='s3_upload_dir', value=s3_dir)

        row = cur.fetchone()


def fnc_check_datalake_env(**kwargs):
    if 1 == 0:
        return 'create_datalake_env'
    return 'cleanup_old_data'


def fnc_create_datalake_env(**kwargs):
    pass

with DAG(
    '03_shona_taxi',
    default_args=default_args,
    description='Shona Taxi data integration DAG',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 1),
    tags=['untapped-global', 'data-integration',
          'shona', 'development'],
) as dag:

    initalize_environment = PythonOperator(
        task_id="initalize_environment",
        python_callable=fnc_initalize_environment,
        provide_context=True,
    )

    check_datalake_env = BranchPythonOperator(
        task_id="check_datalake_env",
        python_callable=fnc_check_datalake_env,
        provide_context=True,
    )

    create_datalake_env = PythonOperator(
        task_id="create_datalake_env",
        python_callable=fnc_create_datalake_env,
        provide_context=True,
    )

    initalize_environment >> check_datalake_env >> create_datalake_env
