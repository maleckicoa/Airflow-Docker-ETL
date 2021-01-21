from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import psycopg2 #PostgreSQL database adapter for python
from airflow.hooks.postgres_hook import PostgresHook #Interaction with the Postgres connection
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from psycopg2.extras import execute_values
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator

from time import sleep
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 11, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['mihajlovic.aleksa@gmail.com']
}



with DAG('1st_dag', description='1stDAG', schedule_interval='*/10 * * * *',catchup=False, default_args=default_args) as dag:



        backup = BashOperator(
        task_id="back_up",
        bash_command= "pg_dump -h 172.17.0.1 -p 5432 --schema=public -U postgres -d analytics > '/usr/local/airflow/tmpdata/backup10.sql'",
        dag=dag)

        clean_public_schema = PostgresOperator(
        postgres_conn_id='postgres_local',
        task_id='clean_public_schema',
        sql="DROP SCHEMA if exists public cascade;",
        autocommit = True,
        dag=dag)

        clean_prod_schema = PostgresOperator(
        postgres_conn_id='postgres_local',
        task_id='clean_prod_schema',
        sql="DROP SCHEMA if exists prod cascade;",
        autocommit = True,
        dag=dag)

        create_public_schema = PostgresOperator(
        postgres_conn_id='postgres_local',
        task_id='create_public_schema',
        sql="CREATE SCHEMA if not exists public;",
        autocommit = True,
        dag=dag)

        restore = BashOperator(
        postgres_conn_id='postgres_local',
        task_id="restore",
        bash_command ="PGPASSWORD='postgres' psql -h 172.17.0.1 -p 5432 -U postgres -d postgres -f /usr/local/airflow/tmpdata/backup10.sql",
        dag=dag)

        rename_public_schema = PostgresOperator(
        postgres_conn_id='postgres_local',
        task_id='rename_public_schema',
        sql="ALTER SCHEMA public rename to prod;",
        autocommit = True,
        dag=dag)

        staging = PostgresOperator(
        postgres_conn_id='postgres_local',
        task_id='staging',
        sql='scripts/staging.sql',
        autocommit = True
        )

        remove_temp_data = BashOperator(
        task_id="remove_temp_data",
        bash_command="rm /usr/local/airflow/tmpdata/backup10.sql",
        dag=dag)

        transportcomplete = DummyOperator(
        task_id="transport_complete",
        dag = dag,
        trigger_rule = 'all_success')

        reporting_metrics = BashOperator(
        task_id="reporting_metrics",
        #bash_command="rm /usr/local/airflow/tmpdata/backup10.sql",
        bash_command="python3 /usr/local/airflow/dags/scripts/reporting_script.py",
        dag=dag)

        parallel_task_1 = DummyOperator(
        task_id="parallel_task_1",
        dag = dag)

        parallel_task_2 = DummyOperator(
        task_id="parallel_task_2",
        dag = dag)

        merge_task = DummyOperator(
        task_id="merge_task",
        dag = dag)




        backup >> clean_public_schema
        clean_public_schema >> clean_prod_schema
        clean_prod_schema >> create_public_schema
        create_public_schema  >> restore
        restore >> rename_public_schema
        rename_public_schema >> staging
        staging >> remove_temp_data
        remove_temp_data >> transportcomplete
        transportcomplete >> reporting_metrics
        reporting_metrics >> parallel_task_1
        reporting_metrics >> parallel_task_2
        merge_task << parallel_task_1
        merge_task << parallel_task_2
        #parallel_task_1 >> merge_task
        #parallel_task_2 >> merge_task
