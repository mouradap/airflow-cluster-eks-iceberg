import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datatype_operations import DatatypePyarrowTransformer



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
    def generate_pyarrow_metadata(response):
        from airflow.models import Variable

        transformer = DatatypePyarrowTransformer(origin_datababase="mysql")
        tables_metadata = {}
        for r in response:
            schema_name, table_name, column_name, data_type = r
            key = f"{schema_name}.{table_name}"
            if key in tables_metadata:
                tables_metadata[key].append({column_name: {"original_datatype": data_type}})
            else:
                tables_metadata[key] = [{column_name: {"original_datatype": data_type}}]

        Variable.set("tables_metadata", tables_metadata)

    generate_pyarrow_metadata(extract_task.output)

main_dag()
