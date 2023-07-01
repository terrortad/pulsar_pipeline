# Importing necessary libraries and modules
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pulsar_producer import pulsar_producer_function
from pulsar_consumer import pulsar_consumer_function


# default args will get passed on to each operator
# setting default arguments that can be applied to the tasks

default_args = {
    'owner': 'airflow',  
    'start_date': days_ago(2), 
    'retries': 1,  #  if a job fails, retry once
    'retry_delay': timedelta(minutes=5),  #  if job fails wait 5 minutes before retrying
}

#  initialize DAG

dag = DAG(
    'pulsar_pipeline',  #  unique identifier of the DAG
    default_args=default_args,  #  passes in default arguments
    description='DAG for Apache Pulsar pipeline', 
    schedule_interval='0 9 * * *',  #  schedule interval in cron syntax. scheduled to run every day at 9am.
)

#  list of cities for which you want to produce and consume data

cities = ['New York'] #  newyork only selected

#  loop through cities, define both tasks

for city in cities:
    #  for each city, define a producer task that calls pulsar_producer_function with that city
    producer_task = PythonOperator(
        task_id=f'produce_task_{city.replace(" ", "_").lower()}',  #  unique identifier of the task, created by combining the string 'produce_task_' with the lowercase city name, and spaces replaced by _
        python_callable=pulsar_producer_function,  #  function to be run by the task
        op_kwargs={'city': city},  # pass the city as an argument to the function
        dag=dag,  #  dag that this task is still apart of
    )

    #  for each city, also define a corresponding consumer task
    consumer_task = PythonOperator(
        task_id=f'consume_task_{city.replace(" ", "_").lower()}',  # The unique identifier of the task, created by combining the string 'consume_task_' with the lowercase city name with spaces replaced by underscores
        python_callable=pulsar_consumer_function,  # The function to be run by the task
        dag=dag,  # The DAG that this task is a part of
    )

    #  consumer_task won't start until producer_task has been completed successfully.
    
    producer_task >> consumer_task
