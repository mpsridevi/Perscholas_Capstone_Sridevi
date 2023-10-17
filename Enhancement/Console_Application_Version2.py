#import neccassary libraries
import pandas as pd
import numpy as np
import colorama
from colorama import Fore, Back, Style
from datetime import datetime
import os

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
    #REQ 2.1 1)Used to display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
    zipcode = input("Please enter zip code: ")
    year_month = input("Enter Year: ") + input("Enter Month: ") + "%"
    print("The transactions made by the customers living in the zip code: ", zipcode)

    output211_df= creditcarddf.join(custinfodf, creditcarddf.CUST_CC_NO == custinfodf.Credit_card_no)\
                            .select("TRANSACTION_ID", "TRANSACTION_VALUE","TRANSACTION_TYPE", col("TIMEID").alias("TRANSACTION DATES"))\
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
            .....Choose the transaction type from the above numbers: """)
    dict_types = {"1":"Bills","2":"Healthcare", 
            "3":"Gas", "4":"Education", 
            "5":"Test", "6":"Entertainment", 
            "7":"Grocery"}
    output_212df = creditcarddf.filter(creditcarddf.TRANSACTION_TYPE == dict_types[type_selected_by_user])\
                       .groupby("TRANSACTION_TYPE")\
                       .agg(
                        count("TRANSACTION_TYPE").alias("NUMBER OF TRANSACTION"), 
                        round(sum("TRANSACTION_VALUE"),2).alias("TOTAL VALUES OF TRANSACTION")
                       )
    return output_212df

def transaction_module_213():
    #REQ 2.1 3)    Used to display the total number and total values of transactions for branches in a given state.
    state_code = input("Please provide 2 letters STATE code: ").upper()
    if(len(state_code)<2 or len(state_code)>2):
        print("Provide the State code in xx format\n")
        state_code = input("Please provide 2 letters STATE code: ").upper()
    print("The state chosen is: ", state_code)
    output213_df =  creditcarddf.join(branchdf, creditcarddf.BRANCH_CODE == branchdf.BRANCH_CODE)\
                        .filter(col("BRANCH_STATE") == state_code)\
                        .groupby("BRANCH_STATE")\
                        .agg(
                            count(creditcarddf.BRANCH_CODE).alias("NUMBER OF BRANCHES"),
                            count(creditcarddf.TRANSACTION_VALUE).alias("NUMBER OF TRANSACTIONS"),
                            round(sum(creditcarddf.TRANSACTION_VALUE),2).alias("TOTAL VALUES OF TRANSACTION")
                            )
    return output213_df

def customer_details_module_221():
    #REQ 2.2 1) Used to check the existing account details of a customer.
    # Collecting the last 4 digits of SSN, Last name and last four digits of Credit card number to display the existing account details.
    ssn_user = "%"+ input("Please enter the last 4 digits of your SSN: ")
    name_user = input("Enter your Last Name: ") 
    #card_user = "%" + input("Enter the last 4 digits of your Credit card Number: ")

    #output221_df= custinfodf.filter((col("LAST_NAME") == name_user) & custinfodf.SSN.like(ssn_user) & custinfodf.Credit_card_no.like(card_user)).dropDuplicates()
    output221_df= custinfodf.filter((col("LAST_NAME") == name_user) & custinfodf.SSN.like(ssn_user)).dropDuplicates()                                                  

    return output221_df

def customer_details_module_222():
    #REQ 2.2 2) Used to modify the existing account details of a customer.
    #Collecting the SSN to make changes
    custinfodf=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=login_key.username,\
                                     password=login_key.password,\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_customer").load()
    
    creditcarddf=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user=login_key.username,\
                                     password=login_key.password,\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_credit_card").load()
    
    print("\nI am happy to help you here, What do you want to update?\n")

    ssn_user = input("\nPlease provide your full SSN: ")
    print("\n")
    options_to_update = input("""\nPlease select from the options:
                                  1. Last name Update, 2. Credit card number update
                                  3. Address update, 4.Phone Update, 5.Email Update:   
                              """)
    print("\n")

    if (int(options_to_update)==1):
        print("LAST NAME UPDATE")
        new_last_name = input("\n Please provide your New Last name          ")
        custinfodf = custinfodf.withColumn("LAST_NAME", when(col("SSN")==ssn_user, new_last_name.title()).otherwise(custinfodf.LAST_NAME))
    elif (int(options_to_update)==2):
        print("Credit Card Number UPDATE")
        new_credit_card_no = input("\n Please provide your New 10 digit Credit Card Number          ")
        custinfodf = custinfodf.withColumn("Credit_card_no", when(col("SSN")==ssn_user, new_credit_card_no).otherwise(custinfodf.Credit_card_no))
        creditcarddf = creditcarddf.withColumn("CUST_CC_NO", when(col("CUST_SSN")==ssn_user,new_credit_card_no).otherwise(creditcarddf.CUST_CC_NO))
    elif(int(options_to_update) == 3):
        print("ADDRESS UPDATE:\n")
        new_full_street_address = input("\nPlease provide your Street address with the apartment number seperated with comma   ")
        new_cust_city = input("\nPlease provide your City   ")
        new_cust_state = input("\nPlease provide your State code   ")
        new_cust_country = input("\nPlease provide your Country    ")
        new_cust_zip = input("\nPlease provide your zipcode    ")

        custinfodf = custinfodf.withColumn("FULL_STREET_ADDRESS", when(col("SSN")==ssn_user, new_full_street_address.title()).otherwise(custinfodf.FULL_STREET_ADDRESS))
        custinfodf = custinfodf.withColumn("CUST_CITY", when(col("SSN")==ssn_user, new_cust_city.title()).otherwise(custinfodf.CUST_CITY))
        custinfodf = custinfodf.withColumn("CUST_STATE", when(col("SSN")==ssn_user, new_cust_state.upper()).otherwise(custinfodf.CUST_STATE))
        custinfodf = custinfodf.withColumn("CUST_COUNTRY", when(col("SSN")==ssn_user, new_cust_country.title()).otherwise(custinfodf.CUST_COUNTRY))
        custinfodf = custinfodf.withColumn("CUST_ZIP", when(col("SSN")==ssn_user, new_cust_zip).otherwise(custinfodf.CUST_ZIP))
        
    elif(int(options_to_update) == 4):
        print("PHONE NUMBER UPDATE")
        new_cust_phone = str(input("\nPlease provide 10 digit phone number   "))
        custinfodf = custinfodf.withColumn("CUST_PHONE", when(col("SSN")==ssn_user, new_cust_phone).otherwise(custinfodf.CUST_PHONE))
        custinfodf = custinfodf.withColumn("CUST_PHONE", when(col("SSN")==ssn_user, concat(lit("("),substring(col("CUST_PHONE"), 1, 3), lit(")"),
                                                                substring(col("CUST_PHONE"), 4, 3), lit("-"),
                                                                substring(col("CUST_PHONE"), 7, 4))).otherwise(custinfodf.CUST_PHONE))

    elif(int(options_to_update) == 5):
        print("EMAIL UPDATE")
        new_cust_email = input("Please provide a valid email id    ")
        custinfodf = custinfodf.withColumn("CUST_EMAIL", when(col("SSN")==ssn_user, new_cust_email).otherwise(custinfodf.CUST_EMAIL))
        

    custinfodf = custinfodf.withColumn("LAST_UPDATED", when(col("SSN")== ssn_user , from_unixtime(current_timestamp().cast("long"))).otherwise(custinfodf.LAST_UPDATED))                                                             

    custinfodf.sort(desc("LAST_UPDATED")).show(1)

def customer_details_module_223():
    #REQ 2.2 3) Used to generate a monthly bill for a credit card number for a given month and year.
    #Collecting the last 4 digits of SSN, Last Name, last 4 digits of credit card number to display the Monthly bill of a user.

    ssn_user = "%"+ input("Please enter the last 4 digits of your SSN: ")
    name_user = input("Enter your Last Name: ") 
    last_4_digits = input("Enter the last 4 digits of your Credit card Number: ")
    card_user = "%" + last_4_digits

    billyear = input("Enter the Year : ")
    billmonth = input("Enter the Month: ")
    #print(billmonth)
    if (int(billmonth)>10):
            curr_month = billyear + billmonth + "%"
            prev_month = billyear +str(int(billmonth)-1) + "%"
            month_list = billmonth
    elif(int(billmonth)==10):
            curr_month = billyear + billmonth + "%"
            prev_month = billyear +"0" + str(int(billmonth)-1) + "%"
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
    print("\n")
    print(f"Monthly Bill for the Credit Card Holder *******{last_4_digits} ---- {firstname} {lastname} ")

    print(f"Billing Period: {months_list[month_list]} {billyear}")
    print("\n")
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

def customer_details_module_224():
    #REQ 2.2 4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
    #Collecting last 4 digits of SSN, last 4 digits of Credit card and 2 dates to display the Transactions.
    ssn_user = "%"+ input("Please enter the last 4 digits of your SSN: ")
    last_4_digits = input("Enter the last 4 digits of your Credit card Number: ")
    card_user = "%" + last_4_digits

    start_date = (input("Enter Starting date YYYYMMDD format: "))
    end_date = (input("Enter Ending date YYYYMMDD format: "))
    output_224df = creditcarddf.filter((col("TIMEID") > start_date ) & (col("TIMEID") < end_date ))\
                            .filter(creditcarddf.CUST_SSN.like(ssn_user) & creditcarddf.CUST_CC_NO.like(card_user))\
                            .withColumn("Date", to_date(col("TIMEID"), "yyyyMMdd"))\
                            .sort(col("TIMEID").desc())\
                            .select( "Date", "BRANCH_CODE", "TRANSACTION_TYPE", "TRANSACTION_VALUE","TRANSACTION_ID")
                        
    print(f"The Transactions happened between {end_date} (YYYYMMDD) and {start_date} (YYYYMMDD) for the customer having the credit card ending with last 4 digits {last_4_digits}:")
    print("\n")
    return output_224df

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

def more_options():
    yes_or_no = input(Fore.RED + Back.WHITE + "    Do you want to see other banking options 'Y' or 'N'   ")
    print(yes_or_no)
    if (yes_or_no == "Y" or yes_or_no == "y"):
        console_application()
    elif(yes_or_no == "N" or yes_or_no =="n"):
        print(Fore.RED + Back.WHITE + "\n It was great to have your here and we hope you enjoyed your experience!!!\n")
        print(Fore.RED + Back.WHITE +  "Thank you and Have a Nice day!!!                                           1\n")
        sys.exit()       
   

def console_application():
    #print("\n")
    #print(Fore.RED + Back.WHITE + "Welcome to Example Bank")
    #print("\n")
    input_from_user=print_options()
    while True:
            if (int(input_from_user) not in [1, 2]):
                print("Please select the correct option")
                input_from_user = print_options()
            else:
                if(input_from_user == 1):
                    transaction_input = transaction_options()
                    
                    if (int(transaction_input) not in [1, 2, 3]):
                        print("Please select the correct option")
                        transaction_input = transaction_options()
                    else:
                        if(int(transaction_input) == 1):
                            output_211df = transaction_module_211()
                            output_211df.show()
                            more_options()
                        elif(int(transaction_input) == 2):
                            output_212df = transaction_module_212()
                            output_212df.show()
                            more_options()
                        elif(int(transaction_input) == 3):
                            output_213df = transaction_module_213()
                            output_213df.show()
                            more_options()

                elif(input_from_user == 2):
                    customer_detail_input = customer_details_options()
                    if (int(customer_detail_input) not in [1, 2, 3, 4]):
                        print("Please select the correct option")
                        customer_detail_input = customer_details_options()
                    else:
                        if(int(customer_detail_input) == 1):
                            output_221df = customer_details_module_221()
                            output_221df.show()
                            more_options()
                        elif(int(customer_detail_input) == 2):
                            customer_details_module_222()
                            more_options()
                        elif(int(customer_detail_input) == 3):
                            customer_details_module_223()
                            more_options()
                        elif(int(customer_detail_input) == 4):
                            output_224df = customer_details_module_224()
                            output_224df.show()
                            more_options()
            
print("\n")
print(Fore.RED + Back.WHITE + "                          Welcome to Example Bank                    ")
print("\n")
console_application()

spark.stop()

           

