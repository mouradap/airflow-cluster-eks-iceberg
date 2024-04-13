import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator



@dag(
    dag_id="extract_metadata",
    start_date=datetime.datetime(2024, 3, 23),
    schedule="0 0 * * *",
    catchup=False
)
def main_dag():
    sql_connection = Variable.get("mysql_connection_id")

    extract_task = MySqlOperator(
        task_id="extract_metadata",
        mysql_conn_id=sql_connection,
        sql="sql/metadata.sql"
    )

    @task()
    def write_to_variables(response):
        from airflow.models import Variable
        Variable.set("tables_metadata", response)

    write_to_variables(extract_task.output)

    
main_dag()