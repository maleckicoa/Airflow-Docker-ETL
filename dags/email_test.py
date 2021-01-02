from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator 


def throw_error(**context):
    raise ValueError('Intentionally throwing an error to send an email.')

default_args = {
    'owner': 'Analytics',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG('email_test',
            max_active_runs=1,
            schedule_interval='@once',
            catchup=False,
            default_args = default_args) as dag:

            t1 = PythonOperator(task_id='throw_error_and_email',
                    python_callable=throw_error,
                    provide_context=True,
                    email_on_failure=True,
                    email='mihajlovic.aleksa@gmail.com',
                    dag=dag)
