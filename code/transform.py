# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, lag, broadcast, collect_set, array_contains, size

# COMMAND ----------

class Transformer:
    def __init__(self):
        pass
    def transform(self, inputDFs):
        pass
    
class AripodsAfterIphoneTransformer(Transformer):
    #Customers who has bought eirpots after buying Uphones
    def transform(self, inputDFs):
        """
        ####Customers who has bought eirpots after buying iphones
        """
        transactionInputDF = inputDFs.get("transactionInputDF")

        print("Showing transaction dataframe")
        transactionInputDF.show()

        #First we need to define window spec, partiononing it by customer_id 
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date") 
    ########################################################################################################################################################################        
        #Lea() is used to get the next value of the column in the same partition (Forward)
        #in this case: What happened AFTER this purchase?
        #              "Find iPhone purchases that came AFTER an AirPods"

        transformDF = transactionInputDF.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
            )
        
        print("Airpods after buying Iphone")
        transformDF.orderBy("customer_id","transaction_date","product_name").show()

        filteredDF = transformDF.filter(
            (col("product_name") == 'iPhone') & (col("next_product_name") == 'AirPods')) #make sure to separate the two statements with paretheses and note that we using lead and not lag

    #######################################################################################################################################################################
        #Lag is used to get the previous value of the column in the same partition (Backward)
        #in this case: What happened BEFORE this purchase?
        #              "Find AirPods purchases that came AFTER an iPhone"

        #transformDF = transactionInputDF.withColumn(
            #"next_product_name", lag("product_name").over(windowSpec)
            #)
        
    ########################################################################################################################################################################
        
        print("filteredDF transformDF")
        filteredDF.orderBy("customer_id","transaction_date","product_name").show()

        #we dont only want customer id we want customer information and it is in a different table thus we need to join the transaction table and customer table
        customerInputDF = inputDFs.get("customerInputDF")

        print("customer table")
        customerInputDF.orderBy("customer_id").show()
        #Instead of doing a normal join we apply the broadcast join which is more efficient
        joinDF = customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
            )    
        print("joined df")
        joinDF.show()
        #joinDF 
        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )  


class OnlyAirpodsandIphoneTransformer(Transformer):
    def transform(self, inputDFs):
        """
        Customeres who have bought only Airpods and Iphone
        """
        transactionInputDF = inputDFs.get("transactionInputDF")
        groupedDF = transactionInputDF.groupBy("customer_id").agg(collect_set("product_name").alias("products"))

        print("groupedDF")
        groupedDF.show()
        
        filteredDF = groupedDF.filter(
            array_contains(col("products"), 'iPhone') & 
            array_contains(col("products"), 'AirPods') & 
            (size(col("products")) == 2)
        )

        
        print("Only Airpods and Iphones")
        filteredDF.show()

        customerInputDF = inputDFs.get("customerInputDF")

        print("customer table")
        customerInputDF.orderBy("customer_id").show()
        #Instead of doing a normal join we apply the broadcast join which is more efficient
        joinDF = customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
            )    
        print("joined df")
        joinDF.show()
        #joinDF 
        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        ) 

class BothAirpodsandIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Customeres who have bought both Airpods and Iphone
        """
        transactionInputDF = inputDFs.get("transactionInputDF")
        print("transactionInputDF")
        transactionInputDF.show()
        
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date") 
        
        transformDF = transactionInputDF.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
            )
        
        filteredDF = transformDF.filter(
            ((col("product_name") == 'iPhone') & (col("next_product_name") == 'AirPods')) | ((col("product_name") == 'AirPods') & (col("next_product_name") == 'iPhone'))) 
        
        print("Airpods after buying Iphone")
        transformDF.orderBy("customer_id","transaction_date","product_name").show()    

        print("filteredDF")
        filteredDF.orderBy("customer_id","transaction_date","product_name").show()

        customerInputDF = inputDFs.get("customerInputDF")
        print("customerInputDF")
        customerInputDF.show()

        joinDF = customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
            )    
        print("joined df")
        joinDF.show()
        #joinDF 
        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )  



# COMMAND ----------

