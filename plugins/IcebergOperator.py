import pyarrow as pa
import pyiceberg.types as pi_types
from pyiceberg.schema import Schema
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import re
from datetime import datetime
import pyarrow.parquet as pq
import io
import boto3
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform
from airflow.exceptions import AirflowException


class IcebergOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        origin_database: str = "mysql",
        to_destination: str = "iceberg",
        aws_conn_id: str = "aws_default",
        s3_bucket: str = "",
        table_name: str = "",
        database_name: str = "",
        table_schema_name: str = "",
        table_metadata: dict = {},
        catalog_namespace: str = "",
        catalog_uri: str = "",
        overwrite_table: bool = True,
        **kwargs,
    ):
        self.origin_database = origin_database.lower()
        self.to_destination = to_destination.lower()
        # Mapping MySQL data types to PyArrow types

        self.transformers = {
            "mysql_to_pyarrow": {
                "varchar": pa.string(),
                "date": pa.date32(),
                "int": pa.int32(),
                "text": pa.string(),
                "decimal": pa.decimal128(
                    38, 18
                ),  # Adjust precision and scale as needed
                "longtext": pa.string(),
                "timestamp": pa.timestamp("us"),
                "time": pa.time64("us"),
                "mediumtext": pa.string(),
                "datetime": pa.timestamp("us"),
                "tinyint": pa.int8(),
                "json": pa.string(),  # JSON is represented as string in PyArrow,
                "blob": pa.large_binary(),
                "tinyblob": pa.large_binary(),
                "mediumblob": pa.large_binary(),
                "longblob": pa.large_binary(),
            },
            "mysql_to_iceberg": {
                "varchar": pi_types.StringType(),
                "date": pi_types.DateType(),
                "int": pi_types.IntegerType(),
                "text": pi_types.StringType(),
                "decimal": pi_types.DecimalType(
                    38, 18
                ),  # Adjust precision and scale as needed
                "longtext": pi_types.StringType(),
                "timestamp": pi_types.TimestampType(),
                "time": pi_types.TimeType(),
                "mediumtext": pi_types.StringType(),
                "datetime": pi_types.TimestampType(),
                "tinyint": pi_types.IntegerType(),
                "json": pi_types.StringType(),  # JSON is represented as string in PyArrow,
                "blob": pi_types.BinaryType(),
                "tinyblob": pi_types.BinaryType(),
                "mediumblob": pi_types.BinaryType(),
                "longblob": pi_types.BinaryType(),
            },
        }
        self.datatype_transformer = self.__transformer()
        self.s3_bucket = s3_bucket
        self.database_name = database_name
        self.table_name = table_name
        self.table_schema_name = table_schema_name
        self.catalog_namespace = catalog_namespace
        self.catalog_uri = catalog_uri
        self.catalog = load_catalog("glue", **{"type": "glue"})
        self.overwrite_table = overwrite_table
        if table_metadata:
            self.table_metadata = table_metadata
        else:
            self.table_metadata = self.__retrieve_metadata_from_airflow_variables(table_name)

        super().__init__(**kwargs)

    
    def __retrieve_metadata_from_airflow_variables(self, table_schema_name, table_name):
        from airflow.models import Variable
        all_metadata = Variable.get("tables_metadata")
        table_key = f"{table_schema_name}.{table_name}"

        table_metadata = all_metadata.get(table_key, None)
        if table_metadata:
            return table_metadata
        raise AirflowException("No metadata found either as input or in tables_metadata Variable.")

    def _read_parquet_from_s3(self, key):
        s3_client = boto3.client("s3")
        s3_obj = s3_client.get_object(Bucket=self.s3_bucket, Key=key)
        table = pq.ParquetFile(io.BytesIO(s3_obj["Body"].read()))
        return table.read()

    def __retrieve_s3_objects(self):
        keys = []

        s3_client = boto3.client("s3")
        s3_prefix = f"{self.database_name}/{self.table_schema_name}/{self.table_name}"

        s3_objects = s3_client.list_objects(Bucket=self.s3_bucket, Prefix=s3_prefix)
        most_recent_extraction = []
        if "Contents" in s3_objects:
            contents = s3_objects["Contents"]
            most_recent_extraction = self.__get_most_recent_date_objects(contents)

        keys = [obj["Key"] for obj in most_recent_extraction]

        return keys

    def __get_date_from_uri(self, uri):
        # Regular expression pattern to match date and time components
        # This pattern assumes the date is in the format YYYY/MM/DD and optionally includes time in HH:MM:SS format
        pattern = r"(\d{4})/(\d{1,2})/(\d{1,2})(?:/(\d{1,2})/(\d{1,2})/(\d{1,2}))?"

        # Search for the pattern in the URI
        match = re.search(pattern, uri)

        if match:
            # Extract the matched groups
            year, month, day, hour, minute, second = match.groups()

            # If hour, minute, or second is not provided, default to 0
            hour = hour or "0"
            minute = minute or "0"
            second = second or "0"

            # Construct the datetime string
            datetime_str = f"{year}/{month}/{day} {hour}:{minute}:{second}"

            # Convert the string to a datetime object
            return datetime.strptime(datetime_str, "%Y/%m/%d %H:%M:%S")
        else:
            # Return None if no match is found
            return None

    def __get_most_recent_date_objects(self, contents):
        most_recent_date = None
        most_recent_date_objects = []
        for s3_object in contents:
            key = object["Key"]

            object_date = self.__get_date_from_uri(key)
            if not most_recent_date or most_recent_date < object_date:
                most_recent_date = object_date
                most_recent_date_objects = [s3_object]
            else:
                most_recent_date_objects.append(s3_object)

        return most_recent_date_objects

    def __transformer(self):
        transformer_model = f"{self.origin_database}_to_{self.to_destination}"
        return self.transformers[transformer_model]

    def transform_datatype(self, dt):
        if dt in self.datatype_transformer:
            return self.datatype_transformer[dt]
        return self.datatype_transformer["varchar"]

    def extract_pyarrow_schema(self, table_metadata):
        pyarrow_schema = None
        pyarrow_schema_fields = []
        for column in table_metadata:
            column_name = list(column.keys())[0]
            column_metadata = column[column_name]
            column_mysql_dt = column_metadata["original_datatype"]
            pyarrow_schema_fields.append(
                pa.field(column_name, self.transform_datatype(column_mysql_dt))
            )

        pyarrow_schema = pa.schema(pyarrow_schema_fields)
        return pyarrow_schema

    def extract_iceberg_schema(self):
        structs = []
        for field_id, column in enumerate(self.table_metadata):
            field_id += 1
            column_name = list(column.keys())[0]
            column_metadata = column[column_name]
            column_mysql_dt = column_metadata["mysql_datatype"]
            field_required = column_metadata.get("field_required", False)
            structs.append(
                {
                    "field_id": field_id,
                    "name": column_name,
                    "type": self.transform_datatype(column_mysql_dt),
                    "required": field_required,
                }
            )

        fields = [pi_types.NestedField(**struct) for struct in structs]
        pyiceberg_schema = Schema(*fields)
        return pyiceberg_schema

    def create_iceberg_table(
        self,
        glue_catalog,
        catalog_uri,
        partition_field_id,
        sort_field_id,
        table_name,
        table_schema,
    ):
        # Specify the Glue Catalog database name and URI
        glue_database_name = glue_catalog
        glue_catalog_uri = catalog_uri  # Replace with your Glue Catalog URI

        # Instantiate glue catalog

        # catalog = load_catalog(catalog_impl="org.apache.iceberg.aws.glue.GlueCatalog", name=glue_database_name, uri=glue_catalog_uri)

        # # Define the partitioning specification with year, month, and day
        # partition_spec = PartitionSpec(
        #     PartitionField(source_id=partition_field_id, transform=YearTransform(), name="partition_year", field_id=len(table_schema)+1),
        #     PartitionField(source_id=partition_field_id, transform=MonthTransform(), name="partition_month", field_id=len(table_schema)+2),
        #     PartitionField(source_id=partition_field_id, transform=DayTransform(), name="partition_day", field_id=len(table_schema)+3)
        # )

        # # Define the sorting order using validTimeUtc field
        sort_order = SortOrder(
            SortField(source_id=sort_field_id, transform=IdentityTransform())
        )

        # Create the Iceberg table using the Iceberg catalog
        self.catalog.create_table(
            identifier=table_name,
            location=catalog_uri,
            schema=table_schema,
            # partition_spec=partition_spec,
            sort_order=sort_order,
        )

        print("Iceberg table created using AWS Glue Catalog.")

    def load_table(self, identifier):
        table_obj = self.catalog.load_table(identifier)
        return table_obj

    def check_table_exists(self):
        current_tables = self.catalog.list_tables(self.catalog_namespace)
        if any(t[1] == self.table_name for t in current_tables):
            return True
        return False

    def transform_pyarrow_schema(self, pa_table, schema):
        pa_table = pa_table.select(schema.names)
        pa_table = pa_table.cast(schema)
        return pa_table

    def execute(self, context):
        self.log.info(f"Initializing IcebergOperator for table {self.table_name}")

        table_exists = self.check_table_exists()
        s3_keys = self.__retrieve_s3_objects()
        if not s3_keys:
            self.log.info("No objects found in S3. Finishing up task")
            return

        pyiceberg_schema = self.extract_iceberg_schema()
        table_object = None

        if not table_exists:
            table_object = self.create_iceberg_table(
                self.catalog_namespace,
                self.catalog_uri,
                self.table_name,
                pyiceberg_schema,
            )

        else:
            identifier = f"{self.catalog_namespace}.{self.table_name}"
            table_object = self.load_table(identifier)

        new_schema = table_object.scan().to_arrow().schema()

        for i, key in enumerate(s3_keys):
            self.log.info(f"Loading chunk: {i+1}")
            pa_table = self._read_parquet_from_s3(key)
            pa_table = self.transform_pyarrow_schema(pa_table, new_schema)
            if self.overwrite_table and i == 0:
                table_object.overwrite(pa_table)
            else:
                table_object.append(pa_table)

        self.log.info("Finished running the loader")
        return
