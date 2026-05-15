# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, firstTransformedDF):
        self.firstTransformedDF = firstTransformedDF
    def sink(self):
        pass

class AirPodsAfterIphoneLoader(AbstractLoader):
    #Defining our overwrite method for loader_factory
    def sink(self):
         get_sink_source(
           sink_type = "dbfs",
           df = self.firstTransformedDF,
           path = "/Volumes/data_engineering_projects/cluster_1/appleanalysisoutput",
           method = "overwrite" 
         ).load_data_frame()

##Write another loader method WITH_PARTITION
class OnlyAirpodsandIPhoneLoader(AbstractLoader):
    #Defining our overwrite method for loader_factory
    def sink(self):
        parameters = {
            "partitionByColumn": ["location"]
        }

        get_sink_source(
            sink_type = "dbfs_with_partition",
            df = self.firstTransformedDF,
            path = "/Volumes/data_engineering_projects/cluster_1/appleanalysisoutput",
            method = "overwrite" ,
            parameters = parameters
         ).load_data_frame()