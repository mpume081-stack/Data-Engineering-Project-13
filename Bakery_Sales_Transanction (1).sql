-- Databricks notebook source
-- MAGIC %md
-- MAGIC # **Bakery Sales Data Analysis & Engineering Pipeline**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Overview**
-- MAGIC ###  This dataset contains sales transactions from a bakery chain across multiple stores. It includes details such as product type, quantity sold, sale date, customer ID, and store location. The data will be used to build a data pipeline for analytics and reporting.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aim
-- MAGIC The aim of this task is to design and implement a structured data pipeline that cleans, transforms, and stores bakery sales data for analysis. This will include validating raw inputs, correcting data formats (such as dates), and preparing outputs for downstream BI tools or dashboards. This project also serves as a practical foundation for mastering SQL transformations and Delta Lake practices.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Goal
-- MAGIC ### The goal is to deliver a reliable and scalable dataset that can power downstream analytics such as:
-- MAGIC
-- MAGIC Tracking daily and monthly sales performance.
-- MAGIC
-- MAGIC Identifying best-selling products and seasonal trends.
-- MAGIC
-- MAGIC Providing insights into customer behavior and purchasing patterns.
-- MAGIC
-- MAGIC Serving as a foundation for further enrichment (e.g., joining with customer or inventory data).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # LOAD DATA

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/bakery_catalog/baker_transanct_schema/bakery_volume

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("dbfs:/Volumes/bakery_catalog/baker_transanct_schema/bakery_volume/Bakery.csv", header=True, inferSchema=True)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CLEAN DATA
-- MAGIC

-- COMMAND ----------

-- Locating the specific schema and catalog
USE CATALOG bakery_catalog;
USE SCHEMA baker_transanct_schema;

-- COMMAND ----------

--creating a temporary view from the csv file
CREATE OR REPLACE TEMPORARY VIEW bakery_sale_view 

USING csv
 OPTIONS(
  path = "dbfs:/Volumes/bakery_catalog/baker_transanct_schema/bakery_volume/Bakery.csv",
  inferSchema = true,
  header = true
);
SELECT * FROM bakery_sale_view LIMIT 10;

-- COMMAND ----------

---querying the temp view
SELECT * FROM bakery_sale_view LIMIT 10;

-- COMMAND ----------

SELECT COUNT(*) AS bakery_items_sold
FROM 
(SELECT Items FROM bakery_sale_view) AS subquery;

-- COMMAND ----------

-- MAGIC %python      ### filtering the Daypart coulumn to only diplay the evening transactions
-- MAGIC df_filter = df.filter(df['Daypart'] == 'Evening')
-- MAGIC display(df_filter)
-- MAGIC        
-- MAGIC

-- COMMAND ----------

---- displaying the number of null values in each column
SELECT

 SUM(CASE WHEN TransactionNo  IS NULL THEN 1 ELSE 0 END) AS null_TransactionNo,
 SUM(CASE WHEN Items IS NULL THEN 1 ELSE 0 END) null_Items,
 SUM(CASE WHEN DateTime IS NULL THEN 1 ELSE 0 END) null_DateTime,
 SUM(CASE WHEN Daypart IS NULL THEN 1 ELSE 0 END) null_Daypart,
 SUM(CASE WHEN DayType IS NULL THEN 1 ELSE 0 END) null_DayType

FROM bakery_sale_view;

-- COMMAND ----------

 --- diplay the number of transaction for each daypart of the Bread Item

SELECT 
 Daypart,
"Bread" AS Items,
 DATE(DateTime) AS  Date_transact,
 COUNT(TransactionNo) AS  Total_Transact
FROM
 bakery_sale_view

 GROUP BY 
  Daypart,  DATE(DateTime);

-- COMMAND ----------

SELECT * FROM bakery_sale_view
WHERE TransactionNo IN ('Bread','Muffin' );

-- COMMAND ----------

SELECT       ---- number of transactions for each daypart of the items column on the weekend from the year of 2016- 2017
    Daypart,
    Items,
    DATE(DateTime) AS Date_Transact,
    COUNT(TransactionNo) AS total_transact
FROM 
    bakery_sale_view
WHERE 
    DAYOFWEEK(DateTime) IN (6, 7)  -- 6 for Saturday and 7 for Sunday
    AND YEAR(DateTime) BETWEEN 2016 AND 2017
GROUP BY 
    Daypart, Items, Date_Transact
ORDER BY 
    Daypart, Items, Date_Transact;

-- COMMAND ----------

SELECT      --- ranking the items column based on the number of transactions
 Items,
 Rank() over( ORDER BY  Items desc) AS rank,
 Dense_Rank() over(ORDER BY  items desc) AS dense_rank,
 Row_Number() over(ORDER BY items desc ) AS  row_number

FROM 
bakery_sale_view;

-- COMMAND ----------

      --- ranking the TransactionNo column based on the number of transactions of Items
 SELECT
  TransactionNo,
  Items,
  Rank() OVER (ORDER BY TransactionNo, Items DESC) AS rank,
  Dense_Rank() OVER (ORDER BY TransactionNo, Items DESC) AS dense_rank,
  Row_Number() OVER (ORDER BY TransactionNo, Items DESC) AS row_number
FROM 
  bakery_sale_view;

-- COMMAND ----------

---- rank the transaction based on the date of the items purchased
SELECT
  TransactionNo,
  Items,
  DateTime ,
  RANK() OVER (PARTITION BY DateTime ORDER BY TransactionNo, Items DESC) AS rank,
  DENSE_RANK() OVER (PARTITION BY DateTime ORDER BY TransactionNo, Items DESC) AS dense_rank,
  ROW_NUMBER() OVER (PARTITION BY DateTime ORDER BY TransactionNo, Items DESC) AS row_number
FROM 
  bakery_sale_view;

-- COMMAND ----------

-- MAGIC %sql    --displaying the clean data or silver table
-- MAGIC CREATE OR REPLACE TABLE bakery_sale 
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   TransactionNo AS Number_Of_Transaction,
-- MAGIC   Items,
-- MAGIC   DateTime AS Date_Transact,
-- MAGIC   Daypart AS DayPart,
-- MAGIC   DayType 
-- MAGIC FROM bakery_sale_view;
-- MAGIC
-- MAGIC SELECT * FROM bakery_sale;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  %restart_python

-- COMMAND ----------

SELECT * FROM bakery_sale LIMIT 10;