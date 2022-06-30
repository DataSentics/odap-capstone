# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercise #1 - Daipe framework basics
# MAGIC 
# MAGIC ## Prerequisties:
# MAGIC - basic understanding of Python and PySpark
# MAGIC - having passed Databricks Capstones
# MAGIC 
# MAGIC ## Daipe framework basics
# MAGIC 
# MAGIC 1. `Bootstrap`
# MAGIC 2. `Imports`
# MAGIC 3. `Decorated functions`
# MAGIC 4. `Loading data`
# MAGIC 5. `Joining data`
# MAGIC 6. `Writing data to a table`

# COMMAND ----------

# MAGIC %md ### Setup - Create A Cluster
# MAGIC 
# MAGIC #### Databricks
# MAGIC 
# MAGIC This capstone project was designed to work with a small cluster. When configuring your cluster, please specify the following:
# MAGIC 
# MAGIC * DBR: **10.4 LTS** 
# MAGIC * Workers: 1
# MAGIC * Node Type: 
# MAGIC   * for Microsoft Azure - **Standard_DS3_v2**
# MAGIC   * for Amazon Web Services - **i3.xlarge** 
# MAGIC   * for Google Cloud Platform - **n1-highmem-4** 
# MAGIC 
# MAGIC Please feel free to use the Community Edition if the recomended node types are not available.

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ../_setup/init

# COMMAND ----------

check_bootstrap()

# COMMAND ----------

import daipe as dp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decorators
# MAGIC ### Simple Python

# COMMAND ----------

def hello_world():
    print("Hello Python")

hello_world()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daipe

# COMMAND ----------

@dp.notebook_function()
def hello_world():
    print("Hello Daipe")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading data

# COMMAND ----------

print(data_source_path)
dbutils.fs.ls(data_source_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load `customers.csv`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple PySpark

# COMMAND ----------

df_customers = spark.read.format("csv").option("header", True).load(data_source_path + "/customers.csv")
display(df_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daipe

# COMMAND ----------

@dp.transformation(dp.read_csv(data_source_path + "/customers.csv", options={"header": True}), display=True)
def load_customers(df):
    return df

# COMMAND ----------

check_load_customers()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join customers with `transactions_2022-06-06.csv`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple PySpark

# COMMAND ----------

df_transactions = spark.read.format("csv").option("header", True).load(data_source_path + "/transactions_2022-06-06.csv")
df_joined = df_customers.join(df_transactions, on="id")
display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daipe

# COMMAND ----------

@dp.transformation(dp.read_csv(data_source_path + "/transactions_2022-06-06.csv", options={"header": True}), display=True)
def load_transactions(df):
    return df

# COMMAND ----------

@dp.transformation(load_customers, load_transactions, display=True)
def join_customers_and_transactions(df_customers, df_transactions):
    return df_customers.join(df_transactions, on="id")

# COMMAND ----------

check_join_customers_and_transactions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a database

# COMMAND ----------

print(db_name)

# COMMAND ----------

from pyspark.sql import SparkSession

@dp.notebook_function()
def create_database(spark: SparkSession):
    spark.sql(f"create database if not exists dev_{db_name}")

# COMMAND ----------

check_database()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save data to a table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple PySpark

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").option("path", "dbfs:/tmp/odap-capstone/bronze/customer_transactions.delta").saveAsTable(f"{db_name}.customer_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daipe

# COMMAND ----------

@dp.transformation(join_customers_and_transactions)
@dp.table_overwrite(f"{db_name}.customer_transactions")
def save_customer_transactions(df):
    return df

# COMMAND ----------

check_save_customer_transactions()

# COMMAND ----------

final_check()
