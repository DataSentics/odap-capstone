# Databricks notebook source
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
# MAGIC 5. `Building pipelines`
# MAGIC 6. `Using pre-configured objects`
# MAGIC 6. `Writing data to a table`

# COMMAND ----------

# MAGIC %md ### Setup - Create or Use An Appropriate A Cluster
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
# MAGIC ### Comparing simple PySpark and Daipe
# MAGIC 
# MAGIC In this Excercise each task has an example solution in simple PySpark just to illustrate  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Run Bootstrap
# MAGIC To be able to run Daipe code it is necessary to first run bootstrap.
# MAGIC 
# MAGIC Learn about what bootstrap does [here](https://datasentics.notion.site/Bootstrap-7afb00d3c5064a9986742ca80ad93cb0)

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ../_setup/init_exercise_01

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
# MAGIC #### Check if bootstrap and imports were successful

# COMMAND ----------

check_bootstrap()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Meet Decorators
# MAGIC 
# MAGIC Daipe is built around __decorated functions__.
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

# write Daipe code to load customers.csv
@dp.transformation(
    dp.read_csv(data_source_path + "/customers.csv", options=dict(header=True)),
    display=True)
def load_customers(df):
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if the load was successful

# COMMAND ----------

check_load_customers()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Building pipelines
# MAGIC #### Join customers with `transactions_2022-06-06.csv`

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

# write Daipe code to load transactions_2022-06-06.csv
@dp.transformation(
    dp.read_csv(
        data_source_path + "/transactions_2022-06-06.csv", options=dict(header=True)
    ),
    display=True,
)
def load_transactions(df):
    return df

# COMMAND ----------

# write Daipe code to join customers and transactions_2022-06-06.csv
@dp.transformation(
    load_customers,
    load_transactions,
    display=True
)
def join_customers_and_transactions(df1, df2):
    return df1.join(df2, on="id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if the join was successful

# COMMAND ----------

check_join_customers_and_transactions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: Saving data
# MAGIC 
# MAGIC Best practice when saving data in Databricks is to save it as a Hive table in the [Delta](https://docs.delta.io/latest/index.html#) format.
# MAGIC 
# MAGIC To do that we first need to create a database.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a database
# MAGIC 
# MAGIC Run the following code to see the name of the database which you need to create to save a table.
# MAGIC 
# MAGIC __Note__: The database name is created from your username to be __unique__ and should not collide with other users.

# COMMAND ----------

print(db_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daipe
# MAGIC 
# MAGIC `TODO`: Create a database called `"dev_" + db_name` (insert `db_name` from the output of cell above) using a `dp.notebook_function()` called `create_database` utilizing the pre-configured objects. Consult [documentation](https://datasentics.notion.site/Using-pre-configured-objects-ac54ac37a12b43478a0256d6b1a6e91c) on how to use pre-configured objects in Daipe.
# MAGIC 
# MAGIC Why the prefix `dev_`? Learn [more](https://datasentics.notion.site/Daipe-in-different-environments-f802146cff5c4ab491204826a32b0211)
# MAGIC 
# MAGIC __Important__: the function must be called `create_database`

# COMMAND ----------

# write Daipe code to create a database
from pyspark.sql import SparkSession
@dp.notebook_function()
def create_database(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS dev_{db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if the database was created properly

# COMMAND ----------

check_database()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7: Writing data to a table
# MAGIC 
# MAGIC When writing data to the file system we rarely want to use the default settings.
# MAGIC 
# MAGIC We __always__ want to use the Delta format.
# MAGIC 
# MAGIC We want to control the __mode__ of writing whether to _overwrite_ or _append_.
# MAGIC 
# MAGIC And we want to have a path where the data is physically saved so that it automatically resolves based on the name of the database and table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple PySpark
# MAGIC To achive this in PySpark we have to specify everything manually like so. 

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").option("overwriteSchema", True).option("path", f"dbfs:/dev/odap-capstone/{db_name}/customer_transactions.delta").saveAsTable(f"dev_{db_name}.customer_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daipe
# MAGIC 
# MAGIC In Daipe most of these options are already taken care of. The user simply picks a decorator of the preferred mode (_append_, _overwrite_ or even _upsert_) and the rest is handled. The path can be configured in `_config/config.yaml` nonetheless it is preconfigured for this exercise so you do not need to change it.
# MAGIC 
# MAGIC `TODO`: write the data produced by the `join_customers_and_transactions` transformation to a table called `"dev_"` + `db_name` + `"customer_transactions"` using Daipe decorators, on repeated calling it should __overwrite__ the table. Consult [documentation](https://datasentics.notion.site/Data-writing-decorators-bccda0b556644e9bac7306b080e01ad3) on how to write data using decorators.
# MAGIC 
# MAGIC __Important__: the decorated function must be called `save_customer_transactions`
# MAGIC 
# MAGIC __Note__: any warnings can be ignored for the purposes of this exercise

# COMMAND ----------

# write Daipe code to save joined customers and transactions to a table
@dp.transformation(join_customers_and_transactions, display=True)
@dp.table_overwrite(f"{db_name}.dev_{db_name}customer_transactions")
def save_customer_transactions(df):
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if the data saved properly

# COMMAND ----------

check_save_customer_transactions()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if everything is done

# COMMAND ----------

final_check()

# COMMAND ----------

# MAGIC %md
# MAGIC #### To make sure everything works in other, clear state of the notebook and Run all cells again
