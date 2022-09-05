# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise #4 - Calculating features
# MAGIC 
# MAGIC ## Prerequisties:
# MAGIC - basic understanding of Python and PySpark
# MAGIC - having passed Databricks Capstones
# MAGIC - having passed Exercise #2 - Feature store basics

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
# MAGIC ### Task 1: Add feature notebooks for calculation
# MAGIC 
# MAGIC Go to `_config/orchestration.yaml` and under `featurestorebundle.orchestration.stages.stage1` add notebook paths per each notebook you want to calculate e.g. `Exercise 02 - Feature store basics` and/or `Exercise 03 - Features with time windows`.
# MAGIC 
# MAGIC Learn more how to configure orchestration [here](https://datasentics.notion.site/Features-orchestration-e81adc3f02ab402b8d7ce1005b4784bc)
# MAGIC 
# MAGIC **Pro tip**: use `%root_module_path%` to get production ready path to the `src` folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Run Bootstrap
# MAGIC To be able to run Daipe code it is necessary to first run bootstrap.
# MAGIC 
# MAGIC Learn about what bootstrap does [here](https://datasentics.notion.site/Bootstrap-7afb00d3c5064a9986742ca80ad93cb0)

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Import Daipe
# MAGIC 
# MAGIC Everything from Daipe can be accessed using one simple import
# MAGIC 
# MAGIC `import daipe as dp`

# COMMAND ----------

import daipe as dp
from box import Box

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Create control widgets
# MAGIC Learn about control widgets [here](https://www.notion.so/datasentics/Core-functionality-aafe501fa561496abd72c062532f16ec#e068537fc9f24f19a999ec1dc2952c7e)

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets_factory: dp.fs.WidgetsFactory):
    widgets_factory.create()
    widgets_factory.create_for_notebooks()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Run calculation
# MAGIC Learn about how features are calculated [here](https://datasentics.notion.site/Features-orchestration-e81adc3f02ab402b8d7ce1005b4784bc)

# COMMAND ----------

@dp.notebook_function(dp.fs.get_stages())
def orchestrate(stages: Box, orchestrator: dp.fs.DatabricksOrchestrator):
    orchestrator.orchestrate(stages)

# COMMAND ----------

# MAGIC %md
# MAGIC ## That's it! Now you can check the Feature store inside the Databricks Feature store UI.
