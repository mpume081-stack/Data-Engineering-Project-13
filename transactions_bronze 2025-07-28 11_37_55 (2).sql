-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 📌 Project Title
-- MAGIC ## Building a Scalable Fraud Detection Data Pipeline on Databricks

-- COMMAND ----------

-- MAGIC %md 📄 Project Overview
-- MAGIC
-- MAGIC This project leverages a realistic, anonymized financial transaction dataset to simulate the development of a data engineering pipeline in Databricks for fraud detection. The dataset reflects typical financial activity, containing transaction details such as amounts, account balances, transaction types, and fraud labels. It is ideal for binary classification and anomaly detection tasks in a real-world financial security context.
-- MAGIC
-- MAGIC The dataset exhibits high class imbalance, where fraudulent transactions (isFraud = 1) are significantly fewer than legitimate ones. This mirrors real-world financial systems, posing both data engineering and machine learning challenges related to data cleaning, transformation, monitoring, and model training.

-- COMMAND ----------

-- MAGIC %md 🎯 Project Goal
-- MAGIC
-- MAGIC The goal of this project is to design, build, and deploy a robust and scalable data pipeline on the Databricks platform that can:
-- MAGIC
-- MAGIC Ingest the raw financial transactions dataset (from Kaggle or CSV source).
-- MAGIC
-- MAGIC Transform and clean the data using SQL and Spark — handling issues like nulls, data types, derived features (accountDiff, isMovement), etc.
-- MAGIC
-- MAGIC Identify class imbalance and prepare the dataset for downstream ML tasks (e.g., using SMOTE or undersampling strategies).
-- MAGIC
-- MAGIC Explore transaction patterns and summarize insights to detect anomalies or potential fraudulent activity.
-- MAGIC
-- MAGIC Enable model development for fraud classification using structured and balanced data.
-- MAGIC
-- MAGIC Persist transformed datasets across the medallion architecture — Bronze (raw) → Silver (cleaned) → Gold (features for ML).
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # LOAD DATA

-- COMMAND ----------

--UPLOAD A VOLUME TO DBFS(DATABRICKS FILE SYSTEM)
%fs ls '/Volumes/fraud_catalog/fraud_schema/fraud_vol'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## CREATE A DATAFRAME
-- MAGIC %python
-- MAGIC df = spark.read.csv("dbfs:/Volumes/fraud_catalog/fraud_schema/fraud_vol/transactions.csv", header=True, inferSchema=True)
-- MAGIC display(df)

-- COMMAND ----------

 -- CREATE A TEMP VIEW
CREATE OR REPLACE TEMPORARY VIEW transactions_bronze
 USING CSV
 OPTIONS(
 path= 'dbfs:/Volumes/fraud_catalog/fraud_schema/fraud_vol/transactions.csv',
 header = True,
 inferSchema = TRUE
 );

SELECT * FROM transactions_bronze;


  


-- COMMAND ----------

-- MAGIC %python    ## SHOW THE TABLE ROWS AND COLUMN NAMES
-- MAGIC (f" Rows:  {df.count()}(), Columns: {len(df.columns)}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DATA CLEANING 

-- COMMAND ----------

-- USE A SPECIFIC CATALOG AND SCHEMA
USE CATALOG fraud_catalog;
USE SCHEMA fraud_schema;

-- COMMAND ----------


-- CHECK THE FOR NULL VALUES IN A COLUMN
SELECT 
  SUM(CASE WHEN step IS NULL THEN 1 ELSE 0 END) AS step_nulls,
  SUM(CASE WHEN type IS NULL THEN 1 ELSE 0 END) AS type_nulls,
  SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_nulls,
  SUM(CASE WHEN nameOrig IS NULL THEN 1 ELSE 0 END) AS nameOrig_nulls,
  SUM(CASE WHEN oldbalanceOrg IS NULL THEN 1 ELSE 0 END) AS oldbalanceOrg_nulls,
  SUM(CASE WHEN newbalanceOrig IS NULL THEN 1 ELSE 0 END) AS newbalanceOrig_nulls,
  SUM(CASE WHEN nameDest IS NULL THEN 1 ELSE 0 END) AS nameDest_nulls,
  SUM(CASE WHEN oldbalanceDest IS NULL THEN 1 ELSE 0 END) AS oldbalanceDest_nulls,
  SUM(CASE WHEN newbalanceDest IS NULL THEN 1 ELSE 0 END) AS newbalanceDest_nulls,
  SUM(CASE WHEN isfraud IS NULL THEN 1 ELSE 0 END) AS isfraud_nulls
FROM transactions_bronze;
  

-- COMMAND ----------

--- DISPLAY TOTAL ROWS AND NULL VALUES IN A COLUMN
SELECT COUNT(*) AS total_rows, COUNT(*) - COUNT(amount) AS missing_amounts FROM transactions_bronze;

-- COMMAND ----------

---- DISPLAY COLUMN DATA TYPE AND COLUMN COMMENT
SELECT * FROM transactions_bronze;
DESCRIBE EXTENDED transactions_bronze;

-- COMMAND ----------

SELECT * FROM transactions_bronze
WHERE type = 'CASH_IN';

-- COMMAND ----------

SELECT 
  step,
  type,
  amount,
  nameOrig,
  oldbalanceOrg,
  newbalanceOrig,
  nameDest,
  oldbalanceDest,
  newbalanceDest,
  isfraud,
  timestampadd(HOUR,step, TIMESTAMP('2023-01-01 00:00:00')) AS transaction_time
  FROM transactions_bronze
  LIMIT 10;
  
SELECT *  FROM transactions_bronze;

-- COMMAND ----------

-- DISPLAY THE BALANCE DIFFERENCE BETWEEN OLD AND NEW BALANCE
CREATE OR REPLACE TEMP VIEW silver_transactions AS 
SELECT *, 
      (oldbalanceOrg - newbalanceOrig) AS account_Diff,
      CASE WHEN amount> 0 THEN 1  ELSE 0 END AS isMovement
FROM transactions_bronze
WHERE amount IS NOT NULL AND oldbalanceOrg IS NOT NULL;

SELECT * FROM silver_transactions;


-- COMMAND ----------

--- 
CREATE OR REPLACE TABLE silver_transactions AS 
SELECT 
  step AS Transaction_Step,
  type AS Transaction_Type,
  amount AS Transaction_Amount,
  nameOrig AS Sender_Id,
  oldbalanceOrg AS Sender_Old_Balance,
  newbalanceOrig AS Sender_New_Balance,
  nameDest AS Recipient_Id,
  oldbalanceDest AS Recipient_Old_Balance,
  newbalanceDest AS Recipient_Rew_Balance,
  isfraud AS Is_Fraud, 
  Account_Diff,
  isMovement AS Is_Movement
  
  

FROM silver_transactions;



-- COMMAND ----------

SELECT isfraud, COUNT(*) FROM silver_transactions GROUP BY isfraud;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_transactions AS 
SELECT 
 
 FROM  silver_transactions
