# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise #3 - Features with time windows
# MAGIC 
# MAGIC ## Prerequisties:
# MAGIC - basic understanding of Python and PySpark
# MAGIC - having passed Databricks Capstones
# MAGIC - having passed Exercise #2 - Feature store basics
# MAGIC 
# MAGIC ## Daipe framework basics
# MAGIC 
# MAGIC 1. `Bootstrap`
# MAGIC 2. `Imports`
# MAGIC 3. `Time windows`
# MAGIC 4. `Widgets`
# MAGIC 5. `feature decorator`
# MAGIC 6. `Loading SDM`
# MAGIC 7. `Adding timestamps`
# MAGIC 8. `Making WindowedDataFrame`
# MAGIC 9. `Writing windowed features`
# MAGIC 10. `Writing more windowed features`

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
# MAGIC ### Task 1: Run Bootstrap
# MAGIC To be able to run Daipe code it is necessary to first run bootstrap.
# MAGIC 
# MAGIC Learn about what bootstrap does [here](https://datasentics.notion.site/Bootstrap-7afb00d3c5064a9986742ca80ad93cb0)

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, creating and filling tables, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ../_setup/init_exercise_03

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Import Daipe
# MAGIC 
# MAGIC Everything from Daipe can be accessed using one simple import
# MAGIC 
# MAGIC `import daipe as dp`
# MAGIC 
# MAGIC Everything needed to work with Time windows is imported using the following import 
# MAGIC 
# MAGIC `from featurestorebundle import time_windows as tw`

# COMMAND ----------

import daipe as dp
from featurestorebundle import time_windows as tw

from pyspark.sql import DataFrame, functions as f

# COMMAND ----------

check_bootstrap()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Task 3: Time windows
# MAGIC 
# MAGIC Time windows are defined in `config.yaml` under the key `featurestorebundle.time_windows`

# COMMAND ----------

@dp.notebook_function("%featurestorebundle.time_windows%")
def show_time_windows(time_windows):
    print(f"Time windows used in this exercise: {time_windows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Create control widgets
# MAGIC Learn about control widgets [here](https://datasentics.notion.site/Core-functionality-aafe501fa561496abd72c062532f16ec#e068537fc9f24f19a999ec1dc2952c7e)

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets_factory: dp.fs.WidgetsFactory):
    widgets_factory.create()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Get entity and feature decorator
# MAGIC Entity holds information about the _entity_ we are about to model in the Feature store. Feature decorator is used to register features into the Feature store. Learn more about the decorator [here](https://datasentics.notion.site/Core-functionality-aafe501fa561496abd72c062532f16ec#2f3691301ff54d778086fd4c341091b0)

# COMMAND ----------

entity = dp.fs.get_entity()
feature = dp.fs.feature_decorator_factory.create(entity)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: Load SDM quality data

# COMMAND ----------

@dp.transformation(dp.read_table(f"{db_name}.customer_transactions_sdm"), display=True)
def load_customer_transactions_sdm(df: DataFrame):
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7: Add timestamps to loaded data
# MAGIC 
# MAGIC `TODO`: fill in appropriate date into the `with_timestamps` function. Consult [documentation](https://datasentics.notion.site/Core-functionality-aafe501fa561496abd72c062532f16ec#fce5c9866062466182a232ddfccbe89c) on the meaning of its arguments.

# COMMAND ----------

@dp.transformation(
    dp.fs.with_timestamps(
        load_customer_transactions_sdm,
        entity,
        # fill in correct argument
    ), display=True
)
def customer_transactions_with_timestamps(df: DataFrame):
    return df

# COMMAND ----------

check_timestamps()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8: Make Windowed DataFrame
# MAGIC 
# MAGIC `TODO`: fill in appropriate date into the `make_windowed` function. Consult [documentation](https://datasentics.notion.site/Time-windows-helper-classes-and-functions-c10623ae913d42e0a60b268b264b45a1#7e6aa18855b146ba95f5580504e46c89) on the meaning of its arguments.

# COMMAND ----------

@dp.transformation(
    tw.make_windowed(
        customer_transactions_with_timestamps,
        entity,
        # fill in correct argument
    ), display=True
)
def customer_transactions_with_time_windows(wdf: tw.WindowedDataFrame):
    return wdf

# COMMAND ----------

check_make_windowed()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9: Write features with time windows
# MAGIC 
# MAGIC `TODO`: Write a transformation which creates a set of features called `sum_amount_{time_window}` and the description of `Sum of amount in last {time_window}` where `NULL` values are filled with 0 using the Windowed Dataframe API. Consult [documentation](https://datasentics.notion.site/How-to-develop-features-with-time-windows-d2dde276e7b94ded9b4925fd4a6a2f08#f9d4ff85fbcf4e4681b94a3a612ab955) on how to do it.
# MAGIC 
# MAGIC __Important__: the decorated function must be called `sum_features` !!!

# COMMAND ----------

# write windowed Daipe code to create features and register them to the Feature store

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if the features were defined correctly

# COMMAND ----------

check_sum_features()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 10: Write more features with time windows
# MAGIC 
# MAGIC `TODO`: Write a transformation which creates a set of features
# MAGIC - `count_amount_{time_window}` and the description of `Count of amount in last {time_window}` where `NULL` values are filled with 0,
# MAGIC - `average_amount_{time_window}` and the description of `Average of amount in last {time_window}` where `NULL` values are filled with 0,
# MAGIC - `average_amount_more_than_5000_{time_window}` and the description of `Average of amount is greater than 5000 in last {time_window}` where `NULL` values are filled with `False` using the Windowed Dataframe API.
# MAGIC 
# MAGIC Also get change features from `average_amount_{time_window}`. Consult [documentation](https://datasentics.notion.site/Change-features-d26e9921ecc04144874fdb2c9ffb51e9) on how to do it.
# MAGIC 
# MAGIC __Important__: the decorated function must be called `amount_features` !!!

# COMMAND ----------

# write windowed Daipe code to create features and register them to the Feature store

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if the features were defined correctly

# COMMAND ----------

check_amount_features()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if everything is done and clean up

# COMMAND ----------

final_check()

# COMMAND ----------

# MAGIC %md
# MAGIC #### To make sure everything works in other, clear state of the notebook and Run all cells again
