#!/bin/bash

# Install additional Python packages

pip install boto3 pandas pyarrow pyiceberg

airflow db init

exec airflow webserver