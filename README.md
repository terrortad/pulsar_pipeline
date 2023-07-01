# pulsar_pipeline
Apache Pulsar Data Pipeline Project

This repository contains Python scripts for a project that retrieves restaurant data using the Yelp API and then routes it using an Apache Pulsar pipeline that uses producers and consumers. To schedule the jobs, the project also incorporates an Apache Airflow Directed Acyclic Graph (DAG).

Getting Started:
Prerequisites
Python 3.x
Apache Pulsar
Apache Airflow
Docker

Files in this repository
-fetch_data.py - This script fetches restaurant data for both New York and Los Angeles, making multiple requests per city to fetch more than just the first 50 results.
-pulsar_city_producer.py - This script includes a function to publish data to Pulsar. It loops over all JSON files in a city's data, sending the data to a Pulsar topic.
-pulsar_city_consumer.py - This script includes a function to create and run a consumer for a given city's Pulsar topic, receiving and processing messages from that topic.
-airflow_dag.py - This script defines an Apache Airflow DAG, which manages the running of producer and consumer tasks for a list of cities.
-
