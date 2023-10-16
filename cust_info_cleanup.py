#import neccassary libraries
import pandas as pd
import numpy as np
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType,VarcharType
from pyspark.sql.functions import *
import requests
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, concat_ws, lit, substring, initcap

#Create Spark Session

spark = SparkSession.builder.appName('SrideviCapstone').getOrCreate()

#Create SparkDataFrame

customerinfodf = spark.read.load("cdw_sapp_custmer.json", format="json", header=True, inferSchema=True)

#Print Schema
customerinfodf.printSchema()
print("Before Data Cleaning")
#print Dataframe
customerinfodf.show(5)

# Convert the First name to Title case
customerinfodf = customerinfodf.withColumn("FIRST_NAME", initcap(col("FIRST_NAME")))
# Convert the Middle name to lower case
customerinfodf = customerinfodf.withColumn("MIDDLE_NAME", lower(col("MIDDLE_NAME")))
# Convert the Last name to Title case
customerinfodf = customerinfodf.withColumn("LAST_NAME", initcap(col("LAST_NAME")))
#Adding the Street name and Apartment
customerinfodf = customerinfodf.withColumn("FULL_STREET_ADDRESS", concat(col("STREET_NAME"), lit(", "), col("APT_NO")))


#Formatting the phone number
customerinfodf = customerinfodf.withColumn("CUST_PHONE", lpad(regexp_replace(col("CUST_PHONE"), "[^0-9]", ""), 10, '123'))

customerinfodf = customerinfodf.withColumn("CUST_PHONE", concat(lit("("),substring(col("CUST_PHONE"), 1, 3), lit(")"),
                                                              substring(col("CUST_PHONE"), 4, 3), lit("-"),
                                                              substring(col("CUST_PHONE"), 7, 4)))

print("After Data Cleaning")
customerinfodf.show(5)