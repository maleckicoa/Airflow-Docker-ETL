from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 11, 1)
}

dag = DAG("email_test", default_args=default_args, schedule_interval=timedelta(days=1))


t1 = EmailOperator(
    task_id="send_mail",
    to='mihajlovic.aleksa@gmail.com',
    subject='Test mail',
    html_content='<p> You have got mail! <p>',
    dag=dag)


def error_function():
    raise Exception('Something wrong')


t2 = PythonOperator(
    task_id='failing_task',
    python_callable=error_function,
    email_on_failure=True,
    email='mihajlovic.aleksa@gmail.com',
    dag=dag,
)
