FROM apache/airflow:2.8.3
COPY ./requirements.txt /opt/airflow/requirements.txt
COPY ./airflow.cfg /opt/airflow/airflow.cfg
RUN pip install --no-cache-dir "apache-airflow==2.8.3" -r /opt/airflow/requirements.txt