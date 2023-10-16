
#import neccassary libraries

import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType,VarcharType
from pyspark.sql.functions import *
import requests, json
import login_key

#Creating a spark session
spark = SparkSession.builder.appName('SrideviCapstone').getOrCreate()

#Functional Requirements 4.1
#Collecting the data from the API
base_url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
response = (requests.get(base_url))
Loan_file = response.json()

#Created a Loan Application Dataframe with the collected data from the API.
Loan_Application_DF = spark.createDataFrame(Loan_file)

#Shows the First 5 rows of data
Loan_Application_DF.show(5)

#Functional Requirements 4.2
#To find the status code of the API point given
print(response)
print(response.status_code)

#Functional Requirements 4.3
#The collected API data needs to be loaded into the Database.
#The table name to be created is CDW-SAPP_loan_application
Loan_Application_DF.write.format("jdbc") \
  .mode("overwrite") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
  .option("user", login_key.username) \
  .option("password", login_key.password) \
  .save()