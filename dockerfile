FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow

#install github provider
RUN pip install 'apache-airflow-providers-github'

#install snowflake provider
RUN pip install 'apache-airflow-providers-snowflake'