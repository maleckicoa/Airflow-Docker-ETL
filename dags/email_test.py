from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def print_hello():
    raise ValueError("Arrays must have the same size")

dag = DAG('email_test', description='Simple tutorial DAG',
          schedule_interval='5 * * * *',
          start_date=datetime(2017, 3, 20))

dummy_operator = DummyOperator(task_id='dummy_task',
                              retries=3,
                              email='mihajlovic.aleksa@gmail.com',
                              email_on_retry=True,
                              dag=dag)

hello_operator = PythonOperator(task_id='hello_task',
                                email='mihajlovic.aleksa@gmail.com',
                                email_on_failure=True,
                                python_callable=print_hello,
                                dag=dag)

dummy_operator >> hello_operator
