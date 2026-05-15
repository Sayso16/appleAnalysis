# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """
    Abstract class for all extractors
    """
    def _init_(self):
        pass
    def extract(self):
        pass

class AppleAnalysisExtractor(Extractor):
    """
    Implement the steps for reading or extrating data
    """
    def extract(self):
        #calling the transaction table firtst
        transactionInputDF  = get_data_source(
            data_type = "csv",
            file_path = "/Volumes/data_engineering_projects/cluster_1/volume_!/Transaction_Updated.csv").read_data_frame()
        transactionInputDF.orderBy("customer_id","transaction_date").show()

        #Calling the customer table
        customerInputDF = get_data_source(
            data_type = "delta",
            file_path = "data_engineering_projects.cluster_1.customer_delta_table").read_data_frame()
            
        #pass the dictionary first
        inputDFs = {
            "transactionInputDF":transactionInputDF,
            "customerInputDF":customerInputDF
                
                    }
        return inputDFs