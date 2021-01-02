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

def print_hello():
        sleep(5)
        return 'Aleksa'

with DAG('1st_dag', description='1stDAG', schedule_interval='*/10 * * * *', start_date=datetime(2018, 11, 1), catchup=False) as dag:

        dummy_task= DummyOperator(task_id='dummy_task', retries=3)

        postgre = PostgresOperator(
        postgres_conn_id='postgres_local',
        task_id='postgre',
        sql="select * from aleksa.phonebook;",
        dag=dag)

        postgre2 = PostgresOperator(
        postgres_conn_id='postgres_default',
        task_id='postgre2',
        sql="select * from aleksa.phonebook;",
        dag=dag)

        backup = BashOperator(
        postgres_conn_id='postgres_local',
        task_id="back_up",
        bash_command= "PGPASSWORD='postgres' pg_dump -h 172.17.0.1 -p 5432 --schema=aleksa -U postgres postgres > '/usr/local/airflow/tmpdata/backup10.sql'",
        dag=dag)

        restore = BashOperator(
        task_id="restore",
        bash_command ="PGPASSWORD='postgres' psql -h 172.17.0.1 -p 5432 -U postgres -d analytics -f /usr/local/airflow/tmpdata/backup10.sql",
        dag=dag
        )



        dummy_task >> postgre >> postgre2 >> backup >> restore



'''

       docke = DockerOperator(
		api_version='auto',
                task_id='docker_command',
                image='postgres:9.6',
                auto_remove=False,
 		        #command="docker exec -it pgdb bash",
                command="pg_dump --schema=aleksa -U airflow airflow  > ddd.sql",
                #docker_url="tcp://localhost:5432",
		        docker_url="unix://var/run/docker.sock",
                network_mode="bridge"
'''
