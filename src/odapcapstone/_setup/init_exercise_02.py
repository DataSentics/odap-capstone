# Databricks notebook source
# pylint: skip-file
import os
import inspect
import ast
from pyspark.sql.utils import AnalysisException
import sys


def checkmark(text):
    return f"""
        <p>
            <div style="float: left; height: 24px">
                <img src="https://emojipedia-us.s3.amazonaws.com/source/skype/289/check-mark-button_2705.png" width="20">
            </div>
            <div style="margin-left: 30px"><p style="font-size: 16px">{text}</p></div>
        </p>
    """


def fail(text):
    return f"""
        <p>
            <div style="float: left; height: 24px">
                <img src="https://emojipedia-us.s3.amazonaws.com/source/skype/289/cross-mark_274c.png" width="20">
            </div>
            <div style="margin-left: 30px"><p style="font-size: 16px">{text}</p></div>
        </p>
    """


def check_bootstrap():
    res_html = ""

    if "APP_ENV" not in os.environ is None or os.environ["APP_ENV"] != "dev":
        res_html += fail("Bootstrap failed")
        displayHTML(res_html)
        return

    res_html += checkmark("APP_ENV=dev")

    if (
        "daipecore" not in sys.modules
        and "datalakebundle" not in sys.modules
        and "databricksbundle" not in sys.modules
        and "injecta" not in sys.modules
    ):
        res_html += fail("Bootstrap installation failed")
        displayHTML(res_html)
        return

    res_html += checkmark("Bootstrap installation successful")

    if "DAIPE_BOOTSTRAPPED" not in os.environ:
        res_html += fail("Bootstrap failed")
        displayHTML(res_html)
        return

    res_html += checkmark("Bootstrap finished successfully")

    if "dp" not in globals() or not hasattr(globals()["dp"], "transformation"):
        res_html += fail("Daipe was not imported correctly")
        displayHTML(res_html)
        return

    res_html += checkmark("Daipe was imported correctly")

    globals()["check_bootstrap_pass"] = True
    displayHTML(res_html)
    

def check_load_customer_transactions_sdm():
    res_html = ""

    expected_dtypes = [('id', 'int'),
 ('first_name', 'string'),
 ('last_name', 'string'),
 ('email', 'string'),
 ('gender', 'string'),
 ('country', 'string'),
 ('city', 'string'),
 ('transaction_date', 'date'),
 ('amount', 'int'),
 ('customer_id', 'int'),
 ('transaction_city', 'string')]

    if "load_customer_transactions_sdm" not in globals():
        res_html += fail("`load_customer_transactions_sdm` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`load_customer_transactions_sdm` found")

    if type(globals()["load_customer_transactions_sdm"]).__name__ != "transformation":
        res_html += fail("`load_customer_transactions_sdm` is not a `transformation`")
        displayHTML(res_html)
        return

    res_html += checkmark("`load_customer_transactions_sdm` is a `transformation`")

    if set(globals()["load_customer_transactions_sdm"].result.dtypes) != set(expected_dtypes):
        res_html += fail("the resulting DataFrame does NOT have the correct schema")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has the correct schema")

    count = globals()["load_customer_transactions_sdm"].result.count()
    if count != 7000:
        res_html += fail(f"the resulting DataFrame number of rows {count} != 7000")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has 7000 rows")
    globals()["check_load_customers_pass"] = True
    displayHTML(res_html)
    

def check_customer_transactions_with_timestamps():
    res_html = ""
    
    expected_dtypes = [('id', 'int'),
 ('timestamp', 'timestamp'),
 ('first_name', 'string'),
 ('last_name', 'string'),
 ('email', 'string'),
 ('gender', 'string'),
 ('country', 'string'),
 ('city', 'string'),
 ('transaction_date', 'date'),
 ('amount', 'int'),
 ('customer_id', 'int'),
 ('transaction_city', 'string')]

    if "customer_transactions_with_timestamps" not in globals():
        res_html += fail("`customer_transactions_with_timestamps` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`customer_transactions_with_timestamps` found")

    if type(globals()["customer_transactions_with_timestamps"]).__name__ != "transformation":
        res_html += fail("`customer_transactions_with_timestamps` is not a `transformation`")
        displayHTML(res_html)
        return

    res_html += checkmark("`customer_transactions_with_timestamps` is a `transformation`")

    code = inspect.getsource(customer_transactions_with_timestamps.function)
    
    parsed_code = ast.parse(code)

    try:
        if parsed_code.body[0].decorator_list[0].args[0].func.attr != "with_timestamps_no_filter":
            res_html += fail(f"incorrect function `{parsed_code.body[0].decorator_list[0].args[0].func.attr}` has been used")
            displayHTML(res_html)
            return
        
        if parsed_code.body[0].decorator_list[0].args[0].args[0].id != "load_customer_transactions_sdm":
            res_html += fail(f"incorrect first argument `{parsed_code.body[0].decorator_list[0].args[0].args[0].id}` has been passed in to `with_timestamps_no_filter`")
            displayHTML(res_html)
            return
        
        if parsed_code.body[0].decorator_list[0].args[0].args[1].id != "entity":
            res_html += fail(f"incorrect second argument `{parsed_code.body[0].decorator_list[0].args[0].args[1].id}` has been passed in to `with_timestamps_no_filter`")
            displayHTML(res_html)
            return
        
    except AttributeError:
        res_html += fail(f"decorator function `with_timestamps_no_filter` has NOT been used correctly")
        displayHTML(res_html)
        return
    
    res_html += checkmark(f"decorator function `with_timestamps_no_filter` has been used correctly")
    
    if set(globals()["customer_transactions_with_timestamps_df"].dtypes) != set(expected_dtypes):
        res_html += fail("the resulting DataFrame does NOT have the correct schema")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has the correct schema")

    count = customer_transactions_with_timestamps_df.count()
    if count != 7000:
        res_html += fail(f"the resulting DataFrame number of rows {count} != 7000")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has 7000 rows")
    globals()["check_customer_transactions_with_timestamps_pass"] = True
    displayHTML(res_html)
    
    
def check_more_than_two_transactions_last_year_flag():
    res_html = ""

    expected_dtypes = [('customer_id', 'int'),
 ('timestamp', 'timestamp'),
 ('more_than_two_transactions_last_year_flag', 'boolean')]

    if "more_than_two_transactions_last_year_flag" not in globals():
        res_html += fail("`more_than_two_transactions_last_year_flag` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`more_than_two_transactions_last_year_flag` found")

    if type(globals()["more_than_two_transactions_last_year_flag"]).__name__ != "transformation":
        res_html += fail("`more_than_two_transactions_last_year_flag` is not a `transformation`")
        displayHTML(res_html)
        return

    res_html += checkmark("`more_than_two_transactions_last_year_flag` is a `transformation`")
    
    if type(more_than_two_transactions_last_year_flag.previous_decorator_instance).__name__ != "feature_decorator":
        res_html += fail("`more_than_two_transactions_last_year_flag` does NOT have a `feature` decorator")
        displayHTML(res_html)
        return

    res_html += checkmark("`more_than_two_transactions_last_year_flag` has a `feature` decorator")
    
    f_defs = len(more_than_two_transactions_last_year_flag.previous_decorator_instance._args)
    if f_defs != 1:
        res_html += fail(f"{f_defs} feature definitions is {'more' if f_defs > 1 else 'less'} than 1")
        displayHTML(res_html)
        return
    
    res_html += checkmark("there is one and only one feature definition")
    
    name_temp = more_than_two_transactions_last_year_flag.previous_decorator_instance._args[0].name_template
    description_temp = more_than_two_transactions_last_year_flag.previous_decorator_instance._args[0].description_template
    if name_temp != "more_than_two_transactions_last_year_flag" or description_temp != "Customer made more than two transactions in the last year":
        res_html += fail("feature definition name and description arguments are wrong")
        displayHTML(res_html)
        return
    
    res_html += checkmark("feature definition name and description arguments are correct")
    
    if more_than_two_transactions_last_year_flag.previous_decorator_instance._args[0].fillna_with != False:
        res_html += fail("feature definition `fillna_with` argument is wrong")
        displayHTML(res_html)
        return
    
    res_html += checkmark("feature definition `fillna_with` argument is correct")
    
    if set(globals()["more_than_two_transactions_last_year_flag"].result.dtypes) != set(expected_dtypes):
        res_html += fail("the resulting DataFrame does NOT have the correct schema")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has the correct schema")

    count = globals()["more_than_two_transactions_last_year_flag"].result.count()
    if count != 998:
        res_html += fail(f"the resulting DataFrame number of rows {count} != 998")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has 998 rows")
    globals()["check_features_pass"] = True
    displayHTML(res_html)


def final_check():
    res_html = ""

    if "check_bootstrap_pass" not in globals() or not globals()["check_bootstrap_pass"]:
        res_html += fail(f"`check_bootstrap` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_bootstrap` passed")

    if (
        "check_load_customers_pass" not in globals()
        or not globals()["check_load_customers_pass"]
    ):
        res_html += fail(f"`check_load_customers` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_load_customers` passed")

    if "check_customer_transactions_with_timestamps_pass" not in globals() or not globals()["check_customer_transactions_with_timestamps_pass"]:
        res_html += fail(f"`check_database` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_customer_transactions_with_timestamps` passed")

    if (
        "check_features_pass" not in globals()
        or not globals()["check_features_pass"]
    ):
        res_html += fail(f"`check_more_than_two_transactions_last_year_flag` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_more_than_two_transactions_last_year_flag` passed")
    displayHTML(res_html)

# COMMAND ----------

import json

user = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["tags"]["user"]
db_name = "odap_academy_" + ''.join(ch for ch in user if ch.isalnum())
del user

# COMMAND ----------

from pyspark.sql import functions as f


df_cust = spark.read.format("csv").option("header", True).load("dbfs:/tmp/odap-capstone/data/customers.csv")

df_t1 = spark.read.format("csv").option("header", True).load("dbfs:/tmp/odap-capstone/data/transactions_2022-05-30.csv")
df_t2 = spark.read.format("csv").option("header", True).load("dbfs:/tmp/odap-capstone/data/transactions_2022-06-01.csv")
df_t3 = spark.read.format("csv").option("header", True).load("dbfs:/tmp/odap-capstone/data/transactions_2022-06-02.csv")
df_t4 = spark.read.format("csv").option("header", True).load("dbfs:/tmp/odap-capstone/data/transactions_2022-06-03.csv")
df_t5 = spark.read.format("csv").option("header", True).load("dbfs:/tmp/odap-capstone/data/transactions_2022-06-04.csv")
df_t6 = spark.read.format("csv").option("header", True).load("dbfs:/tmp/odap-capstone/data/transactions_2022-06-05.csv")
df_t7 = spark.read.format("csv").option("header", True).load("dbfs:/tmp/odap-capstone/data/transactions_2022-06-06.csv")

df_tran = df_t1.unionByName(df_t2).unionByName(df_t3).unionByName(df_t4).unionByName(df_t5).unionByName(df_t6).unionByName(df_t7)

df = (
    df_cust.join(df_tran, on="id")
    .withColumn("id", f.col("id").cast("integer"))
    .withColumn("amount", f.col("amount").cast("integer"))
    .withColumn("customer_id", f.col("customer_id").cast("integer"))
    .withColumn("timestamp", f.from_unixtime(f.col("timestamp") / 1000).cast("date"))
    .withColumnRenamed("timestamp", "transaction_date")
)

df.write.format("delta").mode("overwrite").option("overwriteSchema", True).option("path", f"dbfs:/dev/odap-capstone/{db_name}/customer_transactions.delta").saveAsTable(f"dev_{db_name}.customer_transactions_sdm")

# COMMAND ----------


