import datetime
from airflow.models import Variable
from CustomDbS3Operator import CustomDbS3Operator
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datatype_operations import DatatypePyarrowTransformer
from IcebergOperator import IcebergOperator

@dag(
    dag_id="mysql_to_iceberg",
    start_date=datetime.datetime(2024, 3, 23),
    schedule="0 0 * * *",
    catchup=False
)
def main_dag():
    tables_configuration = Variable.get(
        "mysql_prod_tables_configuration",
        {
            "table_one": {
                "database_name": "prod1",
                "table_schema": "table_schema",
                "table_primary_key": "sid"
            },
            "table_two": {
                "database_name": "prod1",
                "table_schema": "table_schema",
                "table_primary_key": "sid",
                "delta": True,
                "delta_key": "last_updated"
            }
        },
        deserialize_json=True
    )
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

    for table in tables_configuration:
    
        @task_group(table)
        def extract_load_table(table_configuration):

            extract_table = CustomDbS3Operator(
                task_id=f"extract_{table}",
                sql_connection=sql_connection,
                table_name=table,
                database_name=table_configuration.get("database_name"),
                table_schema=table_configuration.get("table_schema"),
                table_primary_key=table_configuration.get("table_primary_key", "sid"),
                aws_bucket="raw-bucket",
                stream=True,
                page_size=10000,
                partition_by="date",
                local=True,
                offset=0,
                delta=table_configuration.get("delta", False),
                delta_key=table_configuration.get("delta_key", None)
            )

            load_to_catalog = IcebergOperator(
                task_id=f"load_table_{table}_to_catalog",
                origin_database="mysql",
                to_destination="iceberg",
                aws_conn_id="aws_default",
                s3_bucket="raw-bucket",
                table_name=table,
                database_name=table_configuration.get("database_name"),
                table_schema_name=table_configuration.get("table_schema"),
                catalog_namespace="datalake-silver",
                catalog_uri="s3://datalake-silver/catalog",
                overwrite_table=True
            )

            extract_table >> load_to_catalog
        
        extract_load_table(tables_configuration[table])
    
main_dag()