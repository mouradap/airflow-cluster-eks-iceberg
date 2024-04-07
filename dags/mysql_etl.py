import datetime
from airflow.decorators import dag
from airflow.models import Variable
from CustomDbS3Operator import CustomDbS3Operator

@dag(
    dag_id="extract_from_s3",
    start_date=datetime.datetime(2024, 3, 23),
    schedule="0 0 * * *"
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
    for table in tables_configuration:
        extract_table = CustomDbS3Operator(
            task_id=f"extract_{table}",
            sql_connection=sql_connection,
            table_name=table,
            database_name=tables_configuration[table].get("database_name"),
            table_schema=tables_configuration[table].get("table_schema"),
            table_primary_key=tables_configuration[table].get("table_primary_key", "sid"),
            aws_bucket="aws_default",
            stream=True,
            page_size=10000,
            partition_by="date",
            local=True,
            offset=0,
            delta=tables_configuration[table].get("delta", False),
            delta_key=tables_configuration[table].get("delta_key", None)
        )

        extract_table

    
main_dag()