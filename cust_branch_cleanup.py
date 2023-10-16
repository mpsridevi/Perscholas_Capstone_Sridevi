#import neccassary libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, concat, lit, substring

#Create Spark Session

spark = SparkSession.builder.appName('SrideviCapstone').getOrCreate()

#Create SparkDataFrame

custbranchdf = spark.read.load("cdw_sapp_branch.json", format="json", header=True, inferSchema=True)

#Print Schema
custbranchdf.printSchema()
print("Before Data Cleaning")
#print Dataframe
custbranchdf.show(5)

#Fill the Zip code to 99999 if the value is NULL
custbranchdf = custbranchdf.na.fill(99999, subset=['BRANCH_ZIP'])

#Formatting the phone number
custbranchdf = custbranchdf.withColumn("BRANCH_PHONE", regexp_replace(col("BRANCH_PHONE"), "[^0-9]", ""))

custbranchdf = custbranchdf.withColumn("BRANCH_PHONE", concat(lit("("),substring(col("BRANCH_PHONE"), 1, 3), lit(")"),
                                                              substring(col("BRANCH_PHONE"), 4, 3), lit("-"),
                                                              substring(col("BRANCH_PHONE"), 7, 4)))

print("After Data Cleaning")
custbranchdf.show(5)


