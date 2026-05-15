# Databricks notebook source


# COMMAND ----------

#define an abstract class  
# So this note book is a factory pattern to read data from any source, in this case is csv, parquet, delta. We can extend this to other extensions later. 
class DataSource:
    """Abstract class for reading data from a source"""
    def __init__(self, path):
        self.path = path
        
    def read_data_frame(self):
        """Abstract method, function will be defined in child classes"""
        raise NotImplementedError("read() method not implemented")

class CSVDataSource(DataSource):
    """Class for reading data from a CSV file"""
    def read_data_frame(self):
        return (
            spark.read.format("csv").
            option("header", "true").
            load(self.path)
            )
class ParquetDataSource(DataSource):
    """Class for reading data from a Parquet file"""
    def read_data_frame(self):
        return (
            spark.read.format("parquet").
            load(self.path)
        )
class DeltaDataSource(DataSource):
    """Class for reading data from a Delta table"""
    
    def read_data_frame(self):
        table_name = self.path
        return (
            spark.read.format("delta").
            table(table_name)
        )
def get_data_source(data_type, file_path):
    if data_type == "csv":
        return CSVDataSource(file_path)
    elif data_type == "parquet":
        return ParquetDataSource(file_path)
    elif data_type == "delta":
        return DeltaDataSource(file_path)
    else:
        raise ValueError("Nor implemented for data_type: {data_type")

# COMMAND ----------

