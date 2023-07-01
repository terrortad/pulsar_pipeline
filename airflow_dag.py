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
    'owner': 'airflow',  # Information about who owns the job
    'start_date': days_ago(2),  # The job should have started 2 days ago
    'retries': 1,  # If a job fails, retry once
    'retry_delay': timedelta(minutes=5),  # If a job fails, wait 5 minutes before retrying
}

# Initialize the DAG
dag = DAG(
    'pulsar_pipeline',  # The unique identifier of the DAG
    default_args=default_args,  # Pass in the default arguments
    description='DAG for Apache Pulsar pipeline',  # Description of what the DAG does
    schedule_interval='0 9 * * *',  # The schedule interval in cron syntax. Here, it's scheduled to run every day at 9am.
)

# A list of cities for which you want to produce and consume data
cities = ['New York']

# Loop through the cities
for city in cities:
    # For each city, define a producer task that calls the pulsar_producer_function with that city
    producer_task = PythonOperator(
        task_id=f'produce_task_{city.replace(" ", "_").lower()}',  # The unique identifier of the task, created by combining the string 'produce_task_' with the lowercase city name with spaces replaced by underscores
        python_callable=pulsar_producer_function,  # The function to be run by the task
        op_kwargs={'city': city},  # Pass the city as an argument to the function
        dag=dag,  # The DAG that this task is a part of
    )

    # For each city, also define a corresponding consumer task
    consumer_task = PythonOperator(
        task_id=f'consume_task_{city.replace(" ", "_").lower()}',  # The unique identifier of the task, created by combining the string 'consume_task_' with the lowercase city name with spaces replaced by underscores
        python_callable=pulsar_consumer_function,  # The function to be run by the task
        dag=dag,  # The DAG that this task is a part of
    )

    # Define task dependencies such that the consumer task depends on the producer task
    # This means the consumer_task won't start until producer_task has been completed successfully.
    producer_task >> consumer_task
