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
# MAGIC * Environment variables:
# MAGIC   * `APP_ENV=dev`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Run Bootstrap
# MAGIC To be able to run Daipe code it is necessary to first run bootstrap.
# MAGIC 
# MAGIC Learn about what bootstrap does [here](https://www.notion.so/datasentics/Bootstrap-7afb00d3c5064a9986742ca80ad93cb0)

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ../_setup/init

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if bootstrap was successful

# COMMAND ----------

check_bootstrap()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Import Daipe
# MAGIC 
# MAGIC Everything from Daipe can be accessed using one simple import
# MAGIC 
# MAGIC `import daipe as dp`

# COMMAND ----------

import daipe as dp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Meet Decorators
# MAGIC 
# MAGIC Daipe is built around __decoratorated functions__.
# MAGIC 
# MAGIC __TL;DR__ Decorators start with a `@` and they are put on top a function definition. They usually take the output of the function and modify it without the function's knowledge. In some cases particularly in Daipe they can provide the function with arguments.
# MAGIC 
# MAGIC If you have never used decorators, learn about how they work [here](https://www.programiz.com/python-programming/decorator)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daipe decorators `hello world`
# MAGIC ### Simple Python
# MAGIC This is how you would write a function in Python. Function has to be __defined__ and then __called__.

# COMMAND ----------

def hello_world():
    print("Hello Python")

hello_world()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daipe
# MAGIC With Daipe decorated functions that is not the case. By using the `@dp.notebook_function()` decorator the function is called immediately after it is defined.
# MAGIC 
# MAGIC There are two top level decorators in Daipe: `@dp.notebook_function()` and ``@dp.transformation()``
# MAGIC  - `@dp.transformation()` is used when the inputs and outputs of the decorated function are DataFrames
# MAGIC  - `@dp.notebook_function()` is a general purpose decorator, it can be used for everything else
# MAGIC  
# MAGIC Learn more about top level decorators [here](https://datasentics.notion.site/Top-level-decorators-e3d015dc9a7a411fa220c90184d24794)

# COMMAND ----------

@dp.notebook_function()
def hello_world():
    print("Hello Daipe")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Best practice
# MAGIC Best practice is to have __exactly one__ decorated function per cell in notebook. This makes for _simpler_, _standardized_ and _self-documenting_ cells in notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Loading data
# MAGIC 
# MAGIC Run the following cell to see where the data files for this exercise are located.

# COMMAND ----------

print(data_source_path)
dbutils.fs.ls(data_source_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load `customers.csv`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple PySpark
# MAGIC 
# MAGIC The following code should be familiar. This is how to load a CSV file in raw PySpark.

# COMMAND ----------

df_customers = spark.read.format("csv").option("header", True).load(data_source_path + "/customers.csv")
display(df_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daipe
# MAGIC 
# MAGIC `TODO`: Load the `customers.csv` in Daipe. Consult [documentation](https://datasentics.notion.site/Data-loading-functions-e6f89bfd2c49473f8fde8bf25f6580bd) about data loading functions if necessary.
# MAGIC 
# MAGIC __Important__: the decorated function must be called `load_customers` !!!

# COMMAND ----------

# load customers.csv

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
# MAGIC 
# MAGIC `TODO`: Load the `transactions_2022-06-06.csv` using Daipe in one decorated function and then pass both the loaded `customers` and `transactions` into another decorated function and perform a join on column `id`. Consult [documentation](https://datasentics.notion.site/Chaining-decorated-functions-633ff0008f5d448dbdc6ef7c9ccac9b9) on how to chain decorated functions if necessary.
# MAGIC 
# MAGIC __Important__: the decorated functions must be called `load_transactions` and `join_customers_and_transactions` !!!

# COMMAND ----------

# load transactions_2022-06-06.csv

# COMMAND ----------

# join customers and transactions_2022-06-06.csv

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
