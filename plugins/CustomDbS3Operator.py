import io
import datetime
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class CustomDbS3Operator(BaseOperator):
    def __init__(
        self,
        sql_connection: str,
        table_name: str,
        table_schema: str,
        database_name: str,
        aws_bucket: str,
        stream: bool = True,
        table_primary_key: str = "id",
        page_size: int = 10000,
        partition_by: str = "date", # Valid: date, datetime
        local: bool = False,
        offset: int = 0,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.sql_connection = sql_connection
        self.table_name = table_name
        self.table_schema = table_schema
        self.database_name = database_name
        self.aws_bucket = aws_bucket
        self.stream = stream
        self.partition_by = partition_by
        self.local = local
        self.table_primary_key = table_primary_key
        self.page_size = page_size
        self.offset = offset

    def __get_sql_hook(self, sql_connection):
        hook = BaseHook.get_hook(sql_connection)
        return hook
    
    def __get_date(self):
        date = datetime.datetime.now()
        year = date.year
        month = date.month
        day = date.day
        hour = date.hour
        minute = date.minute
        second = date.second
        return year, month, day, hour, minute, second
    
    def __create_output_folders(self, local_file_name):
        import os
        output_dir = "/".join(local_file_name.split("/")[:-1])
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def save_df_to_parquet(self, aws_bucket, table_schema, table_name, records_df, offset, local=False):
        year, month, day, hour, minute, second = self.__get_date()
        partition = f"{self.database_name}/{table_schema}/{table_name}"

        if self.partition_by == "date":
            partition += f"/{year}/{month}/{day}"
        elif self.partition_by == "datetime":
            partition += f"/{year}/{month}/{day}/{hour}/{minute}/{second}"
        s3_key = f"{partition}/{offset}.parquet"
        if local:
            local_file_name = f"/opt/airflow/output/{s3_key}"
            self.__create_output_folders(local_file_name)
            self.log.info(f"Saving offset file locally as {local_file_name}")
            records_df.to_parquet(local_file_name, index=False)
        else:
            # Convert DataFrame to Parquet in memory
            buffer = io.BytesIO()
            records_df.to_parquet(buffer, index=False)
            
            # Reset buffer position to the beginning
            buffer.seek(0)
            
            # Initialize the S3Hook
            s3_hook = S3Hook(aws_conn_id='aws_default')

            # Define the S3 bucket and key where the Parquet file will be stored
            s3_key = f"{partition}/{offset}.parquet"
            self.log.info(f"Uploading offset object to S3 as {s3_key}")
            # Upload the Parquet file to S3
            s3_hook.load_file_obj(
                file_obj=buffer,
                key=s3_key,
                bucket_name=aws_bucket,
                replace=True
            )

    def stream_table(self, sql_hook, table_schema, table_name, table_primary_key, page_size, offset):
        while True:
            query = f"SELECT * FROM {table_schema}.{table_name} ORDER BY {table_primary_key} LIMIT {page_size} OFFSET {offset};"
            records_df = sql_hook.get_pandas_df(query)
            if len(records_df) == 0:
                break # No more records to fetch
            self.save_df_to_parquet(
                self.aws_bucket,
                table_schema,
                table_name,
                records_df,
                offset,
                self.local
            )
            offset += page_size

    def execute(self, context):
        self.log.info(f"Running CustomDbS3Operator to load table {self.table_schema}.{self.table_name} to S3")
        self.log.info(f"Local file save set to: {self.local}")

        db_hook = self.__get_sql_hook(self.sql_connection)

        self.stream_table(
            db_hook,
            self.table_schema,
            self.table_name,
            self.table_primary_key,
            self.page_size,
            self.offset
        )
