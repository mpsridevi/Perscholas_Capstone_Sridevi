#import neccassary libraries
import pandas as pd
import numpy as np
from datetime import datetime
from PIL import Image
import time
import colorama
from colorama import Fore, Back, Style
from datetime import datetime

import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType,VarcharType
from pyspark.sql.functions import *
import requests
from pyspark.sql.functions import regexp_replace, col, concat_ws, lit, substring
import login_key
colorama.init(autoreset=True)


#Create Spark Session

spark = SparkSession.builder.appName('SrideviCapstone').getOrCreate()

#Function to read the Customer Branch file and convert it to DataFrame
def extract_branch_file():
    #Create SparkDataFrame
    custbranchdf = spark.read.load("cdw_sapp_branch.json", format="json", header=True, inferSchema=True)
    log("Extracted the cdw_sapp_branch.json file ")
    return custbranchdf

#Function to clean the customer branch file 
def clean_branch_file(custbranchdf):
    #Print Schema
    #custbranchdf.printSchema()
    #print("Before Data Cleaning")
    #print Dataframe
    #custbranchdf.show(5)
    log("Cleaning of the branch file started")
    #Fill the Zip code to 99999 if the value is NULL
    custbranchdf = custbranchdf.na.fill(99999, subset=['BRANCH_ZIP'])

    #Formatting the phone number
    custbranchdf = custbranchdf.withColumn("BRANCH_PHONE", regexp_replace(col("BRANCH_PHONE"), "[^0-9]", ""))

    custbranchdf = custbranchdf.withColumn("BRANCH_PHONE", concat(lit("("),substring(col("BRANCH_PHONE"), 1, 3), lit(")"),
                                                              substring(col("BRANCH_PHONE"), 4, 3), lit("-"),
                                                              substring(col("BRANCH_PHONE"), 7, 4)))

    #print("After Data Cleaning")
    #custbranchdf.show(5)
    log("cleaning of the branch file completed")
    #cleanbranchdf =
    #----------define the StructType and StructFields-------
    #for the below column names
    cleancustbranchdf = custbranchdf.withColumn("BRANCH_CODE",custbranchdf["BRANCH_CODE"].cast(IntegerType()))\
        .withColumn("BRANCH_NAME",custbranchdf["BRANCH_NAME"].cast(VarcharType(50)))\
        .withColumn("BRANCH_STREET",custbranchdf["BRANCH_STREET"].cast(VarcharType(50)))\
        .withColumn("BRANCH_CITY",custbranchdf["BRANCH_CITY"].cast(VarcharType(50)))\
        .withColumn("BRANCH_STATE",custbranchdf["BRANCH_STATE"].cast(VarcharType(50)))\
        .withColumn("BRANCH_ZIP",custbranchdf["BRANCH_ZIP"].cast(IntegerType()))\
        .withColumn("BRANCH_PHONE",custbranchdf["BRANCH_PHONE"].cast(VarcharType(10)))\
        .withColumn("LAST_UPDATED",custbranchdf["LAST_UPDATED"].cast(TimestampType()))
    
    clean_custbranch_DF = cleancustbranchdf.select("BRANCH_CODE", "BRANCH_NAME", "BRANCH_STREET", "BRANCH_CITY", "BRANCH_STATE",\
                                                   "BRANCH_ZIP", "BRANCH_PHONE", "LAST_UPDATED")
    
    return clean_custbranch_DF
 

#Function to read the Credit Card file and convert it to DataFrame
def extract_creditcard_file():
    #Create SparkDataFrame
    creditcarddf = spark.read.load("cdw_sapp_credit.json", format="json", header=True, inferSchema=True)
    return creditcarddf

#Function to clean the creditcard file
def clean_creditcard_file(creditcarddf):
    #Print Schema
    #creditcarddf.printSchema()
    #print("Before Data Cleaning")
    #print Dataframe
    #creditcarddf.show(5)

    #Formatting the Day, Month, Year as TIMEIDYYYYMMDD
    #Method 1:
    #creditcarddf = creditcarddf.withColumn("TIMEID", concat_ws("", col('YEAR'), lpad(col("MONTH"), 2,'0'), lpad(col('DAY'),2,'0')))

    #Method 2:
    date_cols = ["YEAR", lpad("MONTH", 2,'0'), lpad("DAY", 2,'0')]
    creditcarddf = creditcarddf.withColumn("TIMEID", concat_ws("", *date_cols))

    #print("After Data Cleaning")
    #creditcarddf.show(5)

    cleancreditcarddf = creditcarddf.withColumn("CUST_CC_NO",creditcarddf["CREDIT_CARD_NO"].cast(VarcharType(50)))\
        .withColumn("TIMEID",creditcarddf["TIMEID"].cast(VarcharType(50)))\
        .withColumn("CUST_SSN",creditcarddf["CUST_SSN"].cast(IntegerType()))\
        .withColumn("BRANCH_CODE",creditcarddf["BRANCH_CODE"].cast(IntegerType()))\
        .withColumn("TRANSACTION_TYPE",creditcarddf["TRANSACTION_TYPE"].cast(VarcharType(50)))\
        .withColumn("TRANSACTION_VALUE",creditcarddf["TRANSACTION_VALUE"].cast(DoubleType()))\
        .withColumn("TRANSACTION_ID",creditcarddf["TRANSACTION_ID"].cast(IntegerType()))
    
    clean_creditcard_DF = cleancreditcarddf.select("CUST_CC_NO", "TIMEID", "CUST_SSN", "BRANCH_CODE", "TRANSACTION_TYPE", "TRANSACTION_VALUE", "TRANSACTION_ID")
    return clean_creditcard_DF

#Function to read the Customer Information file and convert it to DataFrame
def extract_custinfo_file():
    #Create SparkDataFrame
    customerinfodf = spark.read.load("cdw_sapp_custmer.json", format="json", header=True, inferSchema=True)
    return customerinfodf

def clean_custinfo_file(customerinfodf):
    #Print Schema
    #customerinfodf.printSchema()
    #print("Before Data Cleaning")
    #print Dataframe
    #customerinfodf.show(5)

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

    #print("After Data Cleaning")
    #customerinfodf.show(5)

    #for the below column names
    cleancustinfodf = customerinfodf.withColumn("SSN",customerinfodf["SSN"].cast(IntegerType()))\
        .withColumn("FIRST_NAME",customerinfodf["FIRST_NAME"].cast(VarcharType(50)))\
        .withColumn("MIDDLE_NAME",customerinfodf["MIDDLE_NAME"].cast(VarcharType(50)))\
        .withColumn("LAST_NAME",customerinfodf["LAST_NAME"].cast(VarcharType(50)))\
        .withColumn("Credit_card_no",customerinfodf["CREDIT_CARD_NO"].cast(VarcharType(50)))\
        .withColumn("FULL_STREET_ADDRESS",customerinfodf["FULL_STREET_ADDRESS"].cast(VarcharType(150)))\
        .withColumn("CUST_CITY",customerinfodf["CUST_CITY"].cast(VarcharType(50)))\
        .withColumn("CUST_STATE",customerinfodf["CUST_STATE"].cast(VarcharType(50)))\
        .withColumn("CUST_COUNTRY",customerinfodf["CUST_COUNTRY"].cast(VarcharType(50)))\
        .withColumn("CUST_ZIP",customerinfodf["CUST_ZIP"].cast(IntegerType()))\
        .withColumn("CUST_PHONE",customerinfodf["CUST_PHONE"].cast(VarcharType(10)))\
        .withColumn("CUST_EMAIL",customerinfodf["CUST_EMAIL"].cast(VarcharType(50)))\
        .withColumn("LAST_UPDATED",customerinfodf["LAST_UPDATED"].cast(TimestampType()))
    
    clean_cust_infoDF = cleancustinfodf.select("SSN", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "Credit_card_no", "FULL_STREET_ADDRESS", "CUST_CITY", "CUST_STATE", "CUST_COUNTRY", \
                                               "CUST_ZIP", "CUST_PHONE", "CUST_EMAIL", "LAST_UPDATED" )
    
    return clean_cust_infoDF

def extract_transform_Loading_process():
    custbranchdf = extract_branch_file()
    clean_custbranch_DF = clean_branch_file(custbranchdf)
    clean_custbranch_DF.show(5)

    creditcarddf = extract_creditcard_file()
    clean_creditcard_DF = clean_creditcard_file(creditcarddf)
    clean_creditcard_DF.show(5)

    customerinfodf = extract_custinfo_file()
    clean_cust_infoDF = clean_custinfo_file(customerinfodf)
    clean_cust_infoDF.show(5)

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

#Loading Customer Branch information into the Database.
    clean_custbranch_DF.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
    .option("user", login_key.username) \
    .option("password", login_key.password) \
    .save()

    #Loading the Credit card information into the DataBase
    clean_creditcard_DF.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
    .option("user", login_key.username) \
    .option("password", login_key.password) \
    .save()

    #Loading the Customer Information into the Database
    clean_cust_infoDF.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
    .option("user", login_key.username) \
    .option("password", login_key.password) \
    .save()

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

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

log("==========ETL Job Started==========")
extract_transform_Loading_process()
log("Extract Phase is completed")
log("Transform Phase is completed")
log("Loading Phase is completed")
log("==========ETL Job Ended==========")



#Data Visualization
#Showing all the graphs

Req_3_1_Plot = Image.open('Req_3.1_Plot_Highest_Transaction_Count.png')
Req_3_2_Plot = Image.open('Req_3.2_Plot_Customers_Per_State.png')
Req_3_3_Plot = Image.open('Req_3.3_Plot__Top_Ten_Customers_High_Transaction_Amount.png')

Req_5_1_Plot = Image.open('Req_5.1_Plot_Self_Employed_Application_Status.png')
Req_5_2_Plot = Image.open('Req_5.2_Plot_Married_Male_Application_Status.png')
Req_5_3_Plot = Image.open('Req_5.3_Plot_Top_3_Months_Large_Transaction.png')
Req_5_4_Plot = Image.open('Req_5.4_Plot_Branches_High_Transaction_Healthcare.png')

Req_3_1_Plot.show()
time.sleep(10)

Req_3_2_Plot.show()
time.sleep(10)

Req_3_3_Plot.show()
time.sleep(10)

Req_5_1_Plot.show()
time.sleep(10)

Req_5_2_Plot.show()
time.sleep(10)

Req_5_3_Plot.show()
time.sleep(10)

Req_5_4_Plot.show()
time.sleep(10)

#Data Transaction Modules:

creditcarddf=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=login_key.username,\
                                     password=login_key.password,\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_credit_card").load()

#creditcarddf.show(5)
branchdf=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=login_key.username,\
                                     password=login_key.password,\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_branch").load()

custinfodf=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=login_key.username,\
                                     password=login_key.password,\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_customer").load()

def transaction_module_211():
    zipcode = input("Please enter zip code: ")
    year_month = input("Enter Year: ") + input("Enter Month: ") + "%"
    print(year_month)

    output211_df= creditcarddf.join(custinfodf, creditcarddf.CUST_CC_NO == custinfodf.Credit_card_no)\
                        .select("TRANSACTION_ID", "TRANSACTION_VALUE","TRANSACTION_TYPE","CUST_ZIP","TIMEID")\
                        .filter((col("CUST_ZIP") == zipcode ) & (creditcarddf.TIMEID.like(year_month)))\
                        .dropDuplicates()\
                        .sort(col("TIMEID").desc())
    return output211_df

def transaction_module_212():
    #REQ 2.1 2)    Used to display the number and total values of transactions for a given type.
    type_selected_by_user = input("""\n
            1. Bills \n
            2. Healthcare \n
            3. Gas \n
            4. Education \n
            5. Test \n
            6. Entertainment \n
            7. Grocery \n
            .....Enter Number for type of trasaction: """)
    dict_types = {"1":"Bills","2":"Healthcare", 
            "3":"Gas", "4":"Education", 
            "5":"Test", "6":"Entertainment", 
            "7":"Grocery"}
    output_212df = creditcarddf.filter(creditcarddf.TRANSACTION_TYPE == dict_types[type_selected_by_user])\
                       .groupby("TRANSACTION_TYPE")\
                       .agg(
                        count("TRANSACTION_TYPE").alias("NUMBER_OF_TRANSACTION"), 
                        round(sum("TRANSACTION_VALUE"),2).alias("TOTAL_VALUES_OF_TRANSACTION")
                       )
    return output_212df

def transaction_module_213():
    state_code = input("Please input 2 letters STATE code: ").upper()
    output213_df =  creditcarddf.join(branchdf, creditcarddf.BRANCH_CODE == branchdf.BRANCH_CODE)\
                        .filter(col("BRANCH_STATE") == state_code)\
                        .groupby("BRANCH_STATE")\
                        .agg(
                            count(creditcarddf.BRANCH_CODE).alias("Number of Branches"),
                            count(creditcarddf.TRANSACTION_VALUE).alias("Number_of_Transaction"),
                            round(sum(creditcarddf.TRANSACTION_VALUE),2).alias("Total_Value_of_Transaction")
                            )
    return output213_df

def customer_details_module_221():
    ssn_user = "%"+ input("Please enter the last 4 digits of your SSN: ")
    name_user = input("Enter your Last Name: ") 
    card_user = "%" + input("Enter the last 4 digits of your Credit card Number: ")

    output221_df= custinfodf.filter((col("LAST_NAME") == name_user) & custinfodf.SSN.like(ssn_user) & custinfodf.Credit_card_no.like(card_user)).dropDuplicates()                         

    return output221_df

def customer_details_module_223():
    ssn_user = "%"+ input("Please enter the last 4 digits of your SSN: ")
    name_user = input("Enter your Last Name: ") 
    last_4_digits = input("Enter the last 4 digits of your Credit card Number: ")
    card_user = "%" + last_4_digits

    billyear = input("Enter the Year : ")
    billmonth = input("Enter the Month: ")
    #print(billmonth)
    if (int(billmonth)>=10):
            curr_month = billyear + billmonth + "%"
            prev_month = billyear +str(int(billmonth)-1) + "%"
            month_list = billmonth
    elif(int(billmonth)<10):
        if(len(billmonth)==2):
            prev_month = billyear + "0" +str(int(billmonth)-1) + "%"
            curr_month = billyear + billmonth + "%"
            month_list = billmonth
        else:
            prev_month = billyear + "0" +str(int(billmonth)-1) + "%"
            curr_month = billyear + "0"+ billmonth + "%"
            month_list = "0"+ billmonth


    months_list = {"01":"January","02":"Feburary", 
                "03":"March", "04":"April", 
                "05":"May", "06":"June", 
                "07":"July", "08":"August",
                "09":"September", "10":"October",
                "11":"November", "12":"December"}

    firstname = custinfodf.filter((col("LAST_NAME") == name_user.title()) & custinfodf.SSN.like(ssn_user) & custinfodf.Credit_card_no.like(card_user)).head()[1]
    lastname = custinfodf.filter((col("LAST_NAME") == name_user.title()) & custinfodf.SSN.like(ssn_user) & custinfodf.Credit_card_no.like(card_user)).head()[3]
    credit_card_number = custinfodf.filter((col("LAST_NAME") == name_user.title()) & custinfodf.SSN.like(ssn_user) & custinfodf.Credit_card_no.like(card_user)).head()[4]
                                                                            
    print("\n")
    print("                           ACCOUNT SUMMARY                                                  ")
    print(f"Monthly Bill for the Credit Card Holder *******{last_4_digits} ---- {firstname} {lastname} ")

    print(f"Billing Period: {months_list[month_list]} {billyear}")
    print("Previous Balance:                            ")
    output221_df_prev = creditcarddf.filter((col("CUST_CC_NO") == credit_card_number) & (creditcarddf.TIMEID.like(prev_month)))\
            .withColumn("Date", to_date(col("TIMEID"), "yyyyMMdd"))\
            .select("Date", "TRANSACTION_TYPE", "TRANSACTION_VALUE", "TRANSACTION_ID")
    prev_month_balance = output221_df_prev.agg(round(sum(creditcarddf.TRANSACTION_VALUE),2).alias("PREVIOUS MONTH BALANCE"))
    prev_month_balance.show()
    print("Purchases and Adjustments Balance                            ")
    output221_df_curr = creditcarddf.filter((col("CUST_CC_NO") == credit_card_number) & (creditcarddf.TIMEID.like(curr_month)))\
            .withColumn("Date", to_date(col("TIMEID"), "yyyyMMdd"))\
            .select("Date", "TRANSACTION_TYPE", "TRANSACTION_VALUE", "TRANSACTION_ID")
    Purchases_Adjustments = output221_df_curr.agg(round(sum(creditcarddf.TRANSACTION_VALUE),2).alias("PURCHASES AND ADJUSTMENTS"))
    Purchases_Adjustments.show()
    print("Minimum Payment Due:                           $0\n")
    print("Fees Charged:                                  $0\n")
    print("Interest Charged:                              $0\n")
    print("New Balance:                                      ")
    New_Balance = output221_df_curr.agg(round(sum(creditcarddf.TRANSACTION_VALUE),2).alias("NEW ACCOUNT BALANCE"))
    print("------------------------------------------------------------------------------------------------------")
    New_Balance.show()
    print("------------------------------------------------------------------------------------------------------")
    print("Total Credit Line                                      $10000.00\n")
    print("Cash Credit Line                                       $1100.00\n")
    print("Days in Billing Cycle                                  30\n")

def print_options():
    input_from_user = int(input(""" Please choose from the options \n
                            1. Know my Transaction Details \n
                            2. Customer Details \n"""))
    return input_from_user

def transaction_options():
    transaction_input = int(input(""" \n Please choose one of the options \n
                                  1. Transactions made by customer in a given Zip code\n
                                  2. Display the number and total values of transactions for a given type.\n
                                  3. Display the total number and total values of transactions for branches in a given state\n"""))
    return transaction_input

def customer_details_options():
    customer_detail_input = int(input(""" \n Please choose one of the options \n
                                  1. Check the existing account details of a customer\n
                                  2. Modify the existing account details of a customer\n
                                  3. Generate a monthly bill for a credit card number for a given month and year\n
                                  4. Display the transactions made by a customer between two dates\n"""))
    return customer_detail_input

def console_application():
    print("\n")
    print(Fore.RED + Back.WHITE + "Welcome to Example Bank")
    print("\n")
    
    input_from_user = print_options()

    if(input_from_user == 1):
       transaction_input = transaction_options()
       if(transaction_input == 1):
            output_211df = transaction_module_211()
            output_211df.show()
       elif(transaction_input == 2):
            output_212df = transaction_module_212()
            output_212df.show()
       elif(transaction_input == 3):
            output_213df = transaction_module_213()
            output_213df.show()

    elif(input_from_user == 2):
        customer_detail_input = customer_details_options()
        if(customer_detail_input == 1):
            output_221df = customer_details_module_221()
            output_221df.show()
        elif(customer_detail_input == 3):
            customer_details_module_223()
               

console_application()





