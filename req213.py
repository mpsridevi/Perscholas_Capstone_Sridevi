#import neccassary libraries
import pandas as pd
import numpy as np
from datetime import datetime

import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType,VarcharType
from pyspark.sql.functions import *
import requests
from pyspark.sql.functions import regexp_replace, col, concat_ws, lit, substring
import login_key

#Create Spark Session

spark = SparkSession.builder.appName('SrideviCapstone').getOrCreate()

creditcarddf=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=login_key.username,\
                                     password=login_key.password,\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_credit_card").load()

branchdf=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=login_key.username,\
                                     password=login_key.password,\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_branch").load()

state_code = input("Please input 2 letters STATE code: ").upper()
output213_df =  creditcarddf.join(branchdf, creditcarddf.BRANCH_CODE == branchdf.BRANCH_CODE)\
                        .filter(col("BRANCH_STATE") == state_code)\
                        .groupby("BRANCH_STATE")\
                        .agg(
                            count(creditcarddf.TRANSACTION_VALUE).alias("Number_of_Transaction"),
                            round(sum(creditcarddf.TRANSACTION_VALUE),2).alias("Total_Value_of_Transaction")
                            )

output213_df.show()