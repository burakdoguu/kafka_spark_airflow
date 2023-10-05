from datetime import timedelta
from airflow import DAG
from datetime import datetime
from airflow.decorators import dag

from generate_data_to_kafka import stream_data

from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator

@dag(

    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,    
)

def generate_random_data() -> None:

    start = EmptyOperator(task_id='start')

    stream_task = PythonOperator(
        task_id='fake_data_streaming_kafka',
        python_callable = stream_data
        
    )

    end = EmptyOperator(task_id='end')


    start >> stream_task >> end
    
generate_random_data()