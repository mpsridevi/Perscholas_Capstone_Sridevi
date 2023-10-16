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
from pyspark.sql.functions import regexp_replace, col, concat_ws, lit, substring

#Create Spark Session

spark = SparkSession.builder.appName('SrideviCapstone').getOrCreate()

#Create SparkDataFrame

creditcarddf = spark.read.load("cdw_sapp_credit.json", format="json", header=True, inferSchema=True)

#Print Schema
creditcarddf.printSchema()
print("Before Data Cleaning")
#print Dataframe
creditcarddf.show(5)

#Formatting the Day, Month, Year as TIMEIDYYYYMMDD
#Method 1:
#creditcarddf = creditcarddf.withColumn("TIMEID", concat_ws("", col('YEAR'), lpad(col("MONTH"), 2,'0'), lpad(col('DAY'),2,'0')))

#Method 2:
date_cols = ["YEAR", lpad("MONTH", 2,'0'), lpad("DAY", 2,'0')]
creditcarddf = creditcarddf.withColumn("TIMEID", concat_ws("", *date_cols))

print("After Data Cleaning")
creditcarddf.show(5)