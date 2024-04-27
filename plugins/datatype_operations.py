import pyarrow as pa

class DatatypePyarrowTransformer:
    def __init__(self, origin_database):
        self.origin_database = origin_database
        # Mapping MySQL data types to PyArrow types
        
        self.transformers = {
            "mysql": {
            'varchar': pa.string(),
            'date': pa.date32(),
            'int': pa.int32(),
            'text': pa.string(),
            'decimal': pa.decimal128(38, 18), # Adjust precision and scale as needed
            'longtext': pa.string(),
            'timestamp': pa.timestamp('us'),
            'time': pa.time64('us'),
            'mediumtext': pa.string(),
            'datetime': pa.timestamp('us'),
            'tinyint': pa.int8(),
            'json': pa.string() # JSON is represented as string in PyArrow
            }
        }

        self.datatype_transformer = self.__transformer()
    
    def __transformer(self):
        return self.transformers[self.origin_database]

    def transform_datatype(self, dt):
        if dt in self.datatype_transformer:
            return self.datatype_transformer[dt]
        return pa.string()