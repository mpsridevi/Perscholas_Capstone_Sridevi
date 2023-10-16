#select * from cdw_sapp_branch;
#select  SSN, CUST_STATE from CDW_SAPP_CUSTOMER where CUST_STATE="NY";
#select * from CDW_SAPP_CREDIT_CARD

#select * from cdw_sapp_loan_application;

#select application_status, self_employed from cdw_sapp_loan_application where self_employed="Yes" and application_status = "Y";

#select * from CDW_SAPP_CREDIT_CARD;

#Req 3.2
#select TRANSACTION_TYPE, count(TRANSACTION_TYPE) as NO_OF_TRANSACTIONS from CDW_SAPP_CREDIT_CARD group by TRANSACTION_TYPE;

#Req 5.1
#Find and plot the percentage of applications approved for self-employed applicants.
#select * from cdw_sapp_loan_application
#select Application_status, count(self_employed) from cdw_sapp_loan_application where self_employed="Yes" group by application_status;

#Reg 5.2
#Find the percentage of rejection for married male applicants.
#select Application_Status, count(Married) from cdw_sapp_loan_application where married="Yes" and gender="male" group by application_status;

#Req3.3
#Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
#Hint (use CUST_SSN). 

#SELECT CONCAT(FIRST_NAME, " ", LAST_NAME) AS CUSTOMER_NAME, 
#ROUND(SUM(TRANSACTION_VALUE),2) as VALUE_OF_TRANSACTION 
#FROM CDW_SAPP_CUSTOMER as CUST
#JOIN
#CDW_SAPP_CREDIT_CARD as CREDIT 
#ON CUST.SSN = CREDIT.CUST_SSN
#GROUP BY CUST.SSN, CUSTOMER_NAME
#ORDER BY VALUE_OF_TRANSACTION DESC
#LIMIT 10

#Reg 5.3 Find and plot the top three months with the largest volume of transaction data.
/*SELECT DATE_FORMAT(TIMEID, '%M') As MONTH, ROUND(SUM(TRANSACTION_VALUE),2) AS TOTAL_TRANSACTIONS
FROM CDW_SAPP_CREDIT_CARD
GROUP BY MONTH
ORDER BY TOTAL_TRANSACTIONS DESC
LIMIT 4*/

#SELECT TIMEID, TRANSACTION_VALUE FROM CDW_SAPP_CREDIT_CARD



#Reg 5.4 Find and plot which branch processed the highest total dollar value of healthcare transactions.

#SELECT BRANCH_CITY, ROUND(SUM(TRANSACTION_VALUE),2) AS  TOTAL_TRANSACTION_VALUE
#FROM CDW_SAPP_BRANCH as BRANCH
#JOIN CDW_SAPP_CREDIT_CARD AS CREDITCARD
#ON BRANCH.BRANCH_CODE = CREDITCARD.BRANCH_CODE
#WHERE CREDITCARD.TRANSACTION_TYPE="HEALTHCARE"
#GROUP BY BRANCH_CITY
#ORDER BY TOTAL_TRANSACTION_VALUE DESC
#LIMIT 5

select * from cdw_sapp_branch;

select * from cdw_sapp_customer;

select * from cdw_sapp_credit_card










