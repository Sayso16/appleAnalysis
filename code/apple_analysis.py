# Databricks notebook source
# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

# DBTITLE 1,Minimal fix: Only show if not None
#now we are creating workflow class
#In the work flow class we will be calliing all the ETL classes
#We are also using LLD to read from different sources
class AripodsAfterIphoneWorkflow:
    """
    ETL Pipeline to generate the data for: all customers who have bought airpods just after buying iphones
    """
    def __init__(self):
         pass
  
    def runner(self):

        #Step 1: Extracting all the required data from  
        #different sources
        inputDFs = AppleAnalysisExtractor().extract()

        #Step 2:Implementing the transformation logic:
        # Customers who has bought eirpots after buying Uphones
        firstTransformedDF = AripodsAfterIphoneTransformer().transform(inputDFs)

        #Step 3: loading all the required data to different sink
        AirPodsAfterIphoneLoader(firstTransformedDF).sink()
            
    #so for getting the data frame, we need to call the particular function  of CSV class 

# COMMAND ----------

# DBTITLE 1,Cell 1
class OnlyAirpodsandIphoneWorkflow:
    """
    ETL Pipeline to generate the data for: all customers who have bought only  airpods and iphones 
    """
    def __init__(self):
        pass
  
    def runner(self):

        #Step 1: Extracting all the required data from  
        #different sources
        inputDFs = AppleAnalysisExtractor().extract()

        #Step 2:Implementing the transformation logic:
        # Customers who has bought eirpots after buying Uphones
        secondTransformedDF = OnlyAirpodsandIphoneTransformer().transform(inputDFs)

        #Step 3: loading all the required data to different sink
        OnlyAirpodsandIPhoneLoader(secondTransformedDF).sink()
        
    #so for getting the data frame, we need to call the particular function  of CSV class 

# COMMAND ----------

# DBTITLE 1,Cell 6
class BothAirpodsandIphoneWorkFlow:
    """
    ETL Pipeline to generate the data for: all customers who have bought BOTH  airpods and iphones 
    """
    def __init__(self):
        pass
    
    def runner(self):
        #Step 1: Extracting all the required data 
        inputDFs = AppleAnalysisExtractor().extract()

        #Step 2: Implementing the transformation logic for customers who have bought both airpods and iphones
        thirdTransformedDF = BothAirpodsandIphoneTransformer().transform(inputDFs)

        #Step 3: Storing the data into the required format
        AirPodsAfterIphoneLoader(thirdTransformedDF).sink()
    

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Cell 6
#We have more than one workflow, so we define a Workflow runner class to run all the workflows, will help call the required workflow Basically a scheduler
class WorkFlowRunner:
    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "aripodsAfterIphoneWorkFlow":
            return AripodsAfterIphoneWorkflow().runner()
        elif self.name == "onlyAripodsAfterIphoneWorkFlow":
            return OnlyAirpodsandIphoneWorkflow().runner()
        elif self.name == "bothAripodsAfterIphoneWorkFlow":
            return BothAirpodsandIphoneWorkFlow().runner()
        else:
            raise ValueError(f"Not implemented for {self.name}")


name = "onlyAripodsAfterIphoneWorkFlow"

workFlowRunner = WorkFlowRunner(name).runner()