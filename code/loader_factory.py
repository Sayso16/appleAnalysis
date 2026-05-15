# Databricks notebook source
##Same with factory pattern (reader_factor) we can implement out factory pattern to load the data
#This will reduce writing the moduler code and we will implement on lld which is factory pattern

class DataSink:
    """
    Abstract class for data sink
    """

    def __init__(self, df, path, method, parameters): ##Define a method for load (the type of method to use to load)
        self.df = df 
        self.path = path
        self.method = method
        self.parameters = parameters ## this is for the case were we might require extenal parameters


    def load_data_frame(self):
        """
        Abstract methods, funtion will be defined in sub classes
        """
        raise ValueError("Not Implemente")

class LoadToDBFS(DataSink):
    def load_data_frame(self):
        ##We dont need to return it we can simply load it
        self.df.write.mode(self.method).save(self.path)
    
class LoadToDBFSWithPartition(DataSink):
    def load_data_frame(self):
        ##We dont need to return it we can simply load it
        partitionByColumns = self.parameters.get("partitionByColumn")
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)

def get_sink_source(sink_type,df , path, method, parameters = None):
    if sink_type == "dbfs":
        return  LoadToDBFS(df , path, method, parameters)
        
    elif sink_type == "dbfs_with_partition":
        return  LoadToDBFS(df , path, method, parameters)
    else:
        raise ValueError(f"Not implemented for sink type:")

# COMMAND ----------

    ##What is partition: if you keep every file in a single folder it will be very hard to scan : whenever you a firing a query (eg Select * from this particular table ) and you are passing the location, most of the filter conditions contains filters on location columns. Thus is better to create multiple sub-folders based on the location column and then scan the data. So we create a partitionBy on that folder: so instead of keeping all the files in a single folder, i will create mutliple sub folders wil location = a, location = b, location = c e.t.c, and it will keep the corresponding file like (CSV, Parque, JSON) that particular location

    #Partitioning is a technique used in distributed computing to divide a large dataset into smaller chunks, based on the values of one or more columns, that can be processed in parallel. In the context of data lakes, partitioning is often used to improve query performance by allowing the system to quickly locate the relevant data based on specific criteria.


    #Bucketing apply when the number o columns is very high. This technique divide data into managable chunks based on the hash value of a specific column, unlike partition that spplit sata into directorires based on column value.