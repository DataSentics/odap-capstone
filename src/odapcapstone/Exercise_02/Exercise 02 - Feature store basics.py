# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise #2 - Feature store basics
# MAGIC 
# MAGIC ## Prerequisties:
# MAGIC - basic understanding of Python and PySpark
# MAGIC - having passed Databricks Capstones
# MAGIC - having passed Exercise #1 - Daipe framework basics
# MAGIC 
# MAGIC ## Daipe framework basics
# MAGIC 
# MAGIC 1. `Bootstrap`
# MAGIC 2. `Imports`
# MAGIC 3. `Entity and target store`
# MAGIC 4. `Widgets`
# MAGIC 5. `feature decorator`
# MAGIC 6. `Loading SDM`
# MAGIC 7. `Adding timestamps`
# MAGIC 8. `Writing features`

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
# MAGIC Learn about what bootstrap does [here](https://www.notion.so/datasentics/Bootstrap-7afb00d3c5064a9986742ca80ad93cb0)

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, creating and filling tables, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ../_setup/init_exercise_02

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

check_bootstrap()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Task 3: Configure entity and Init target store
# MAGIC 
# MAGIC Go to `_config/config.yaml` and under `featurestorebundle.entities` change `name_of_entity` to some unique name for example __your name__. Learn more about entities [here](https://www.notion.so/datasentics/How-to-develop-features-bb486a6d961b43c18ea9dda2ee7cd628#330054af9514407cb1a326a4b23a93ac).
# MAGIC 
# MAGIC Then run this [notebook]($../_setup/init_target_store) __once__ to initialize _target store_. Target store is necessary for using Feature store. Learn more about Target store in [documentation](https://datasentics.notion.site/Target-store-3e7756b00b884335b1132d3c927459c1). 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Create control widgets
# MAGIC Learn about control widgets [here](https://www.notion.so/datasentics/Core-functionality-aafe501fa561496abd72c062532f16ec#e068537fc9f24f19a999ec1dc2952c7e)

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets_factory: dp.fs.WidgetsFactory):
    widgets_factory.create()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Get entity and feature decorator
# MAGIC Entity holds information about the _entity_ we are about to model in the Feature store. Feature decorator is used to register features into the Feature store. Learn more about the decorator [here](https://www.notion.so/datasentics/Core-functionality-aafe501fa561496abd72c062532f16ec#2f3691301ff54d778086fd4c341091b0)

# COMMAND ----------

entity = dp.fs.get_entity()
feature = dp.fs.feature_decorator_factory.create(entity)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: Load SDM quality data
# MAGIC `TODO`: Load table `db_name` + `"customer_transactions_sdm"` using Daipe. Consult [documentation](https://datasentics.notion.site/Chaining-decorated-functions-633ff0008f5d448dbdc6ef7c9ccac9b9) on how to chain decorated functions if necessary.
# MAGIC 
# MAGIC __Important__: the decorated function must be called `load_customer_transactions_sdm` !!!

# COMMAND ----------

print(db_name)

# COMMAND ----------

# write Daipe code to load table customer_transactions_sdm

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if SDM was loaded correctly

# COMMAND ----------

check_load_customer_transactions_sdm()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7: Add timestamps to loaded data
# MAGIC 
# MAGIC Feature store needs a `timestamp` column to be able to register features. Learn more about why that is [here](https://datasentics.notion.site/How-to-develop-features-bb486a6d961b43c18ea9dda2ee7cd628#fd526e2788984e398dca4bbf8bdd7ae2)
# MAGIC 
# MAGIC `TODO`: Create a Daipe decorated function to add `timestamps` to the load SDM data.
# MAGIC 
# MAGIC __Important__: the decorated function must be called `customer_transactions_with_timestamps` !!!

# COMMAND ----------

# write Daipe code to add timestamps to the output of load_customer_transactions_sdm

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if timestamps were added correctly

# COMMAND ----------

check_customer_transactions_with_timestamps()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8: Write a simple feature
# MAGIC 
# MAGIC `TODO`: Write a transformation which creates a feature column called `more_than_two_transactions_last_year_flag` which is `True` if the customer made more than __two__ transactions and `False` otherwise, and register it into the Feature store.
# MAGIC Use `Customer made more than two transactions in the last year` as the description and fill empty values with `False`.
# MAGIC Consult [documentation](https://datasentics.notion.site/How-to-develop-features-bb486a6d961b43c18ea9dda2ee7cd628#83980cb985a3435ca428dc8f92146950) on how to write features.
# MAGIC 
# MAGIC __Important__: the decorated function must be called `more_than_two_transactions_last_year_flag` !!!

# COMMAND ----------

# write Daipe code to create features and register them to the Feature store

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if the features were defined correctly

# COMMAND ----------

check_more_than_two_transactions_last_year_flag()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if everything is done and clean up

# COMMAND ----------

final_check()

# COMMAND ----------

# MAGIC %md
# MAGIC #### To make sure everything works in other, clear state of the notebook and Run all cells again
