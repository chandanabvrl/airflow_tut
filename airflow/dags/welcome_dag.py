from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print('Today is {}'.format(datetime.today().date()))

def print_random_quote():
    response = requests.get('https://api.quotable.io/random')
    quote = response.json()['content']
    print('Quote of the day: "{}"'.format(quote))

default_args = {
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='welcome_dag',
    default_args=default_args,
    schedule='0 23 * * *',  # every day at 11 PM
    catchup=False
) as dag:

    print_welcome_task = PythonOperator(
        task_id='print_welcome',
        python_callable=print_welcome
    )

    print_date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date
    )

    print_random_quote_task = PythonOperator(
        task_id='print_random_quote',
        python_callable=print_random_quote
    )

    # Set task dependencies
    print_welcome_task >> print_date_task >> print_random_quote_task
