# Databricks notebook source
# pylint: skip-file
import json

user = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["tags"]["user"]
db_name = "odap_academy_" + ''.join(ch for ch in user if ch.isalnum())
del user

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

def check_timestamps():
    res_html = ""

    if "customer_transactions_with_timestamps" not in globals():
        res_html += fail("`customer_transactions_with_timestamps` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`customer_transactions_with_timestamps` found")

    if customer_transactions_with_timestamps_df.count() != 6000:
        res_html += fail("`customer_transactions_with_timestamps_df` count is NOT 6000")
        displayHTML(res_html)
        return
    
    res_html += checkmark("`customer_transactions_with_timestamps_df` count is 6000")
    
    globals()["check_timestamps_pass"] = True
    displayHTML(res_html)
    
def check_make_windowed():
    res_html = ""

    if "customer_transactions_with_time_windows" not in globals():
        res_html += fail("`customer_transactions_with_time_windows` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`customer_transactions_with_time_windows` found")
    
    # ===========================================================================

    if customer_transactions_with_timestamps_df.count() != 6000:
        res_html += fail("`customer_transactions_with_time_windows_df` count is NOT 6000")
        displayHTML(res_html)
        return
    
    res_html += checkmark("`customer_transactions_with_time_windows_df` count is 6000")
    
    # ===========================================================================

    if type(customer_transactions_with_time_windows_df).__name__ != "WindowedDataFrame":
        res_html += fail("`customer_transactions_with_time_windows_df` type is NOT `WindowedDataFrame`")
        displayHTML(res_html)
        return
    
    res_html += checkmark("`customer_transactions_with_time_windows_df` type is `WindowedDataFrame`")
    
    # ===========================================================================

    if customer_transactions_with_time_windows_df.time_windows != ['3d', '5d']:
        res_html += fail("time_windows in `WindowedDataFrame` are NOT ['3d', '5d']")
        displayHTML(res_html)
        return
    
    res_html += checkmark("time_windows in `WindowedDataFrame` are ['3d', '5d']")
    
    # ===========================================================================

    if customer_transactions_with_time_windows_df.__dict__["_WindowedDataFrame__time_column_name"] != "transaction_date":
        res_html += fail("time_column_name in `WindowedDataFrame` is NOT `transaction_date`")
        displayHTML(res_html)
        return
    
    res_html += checkmark("time_column_name in `WindowedDataFrame` is `transaction_date`")
    
    globals()["check_make_windowed_pass"] = True
    displayHTML(res_html)
    
def check_sum_features():
    res_html = ""

    expected_dtypes = [('customer_id', 'int'),
 ('timestamp', 'timestamp'),
 ('sum_amount_3d', 'bigint'),
 ('sum_amount_5d', 'bigint')]

    if "sum_features" not in globals():
        res_html += fail("`sum_features` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`sum_features` found")

    if type(globals()["sum_features"]).__name__ != "transformation":
        res_html += fail("`sum_features` is not a `transformation`")
        displayHTML(res_html)
        return

    res_html += checkmark("`sum_features` is a `transformation`")
    
    if type(sum_features.previous_decorator_instance).__name__ != "feature_decorator":
        res_html += fail("`sum_features` does NOT have a `feature` decorator")
        displayHTML(res_html)
        return

    res_html += checkmark("`sum_features` has a `feature` decorator")
    
    f_defs = len(sum_features.previous_decorator_instance._args)
    if f_defs != 1:
        res_html += fail(f"{f_defs} feature definitions is {'more' if f_defs > 1 else 'less'} than 1")
        displayHTML(res_html)
        return
    
    res_html += checkmark("there is one and only one feature definition")
    
    name_temp = sum_features.previous_decorator_instance._args[0].name_template
    description_temp = sum_features.previous_decorator_instance._args[0].description_template
    if name_temp != "sum_amount_{time_window}" or description_temp != "Sum of amount in last {time_window}":
        res_html += fail("feature definition name and description arguments are wrong")
        displayHTML(res_html)
        return
    
    res_html += checkmark("feature definition name and description arguments are correct")
    
    if sum_features.previous_decorator_instance._args[0].fillna_with != 0:
        res_html += fail("feature definition `fillna_with` argument is wrong")
        displayHTML(res_html)
        return
    
    res_html += checkmark("feature definition `fillna_with` argument is correct")
    
    if set(globals()["sum_features"].result.dtypes) != set(expected_dtypes):
        res_html += fail("the resulting DataFrame does NOT have the correct schema")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has the correct schema")

    count = globals()["sum_features"].result.count()
    if count != 998:
        res_html += fail(f"the resulting DataFrame number of rows {count} != 998")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has 998 rows")
    
    if sum_features_df.groupBy().agg(f.sum(f.col("sum_amount_3d").cast("integer"))).collect()[0][0] != 22113457:
        res_html += fail(f"the resulting feature is NOT calculated correctly")
        displayHTML(res_html)
        return
    
    res_html += checkmark("the resulting feature is calculated correctly")
    
    globals()["check_sum_features_pass"] = True
    displayHTML(res_html)

def check_amount_features():
    res_html = ""

    expected_dtypes = [('customer_id', 'int'),
 ('timestamp', 'timestamp'),
 ('count_amount_3d', 'bigint'),
 ('avg_amount_3d', 'double'),
 ('count_amount_5d', 'bigint'),
 ('avg_amount_5d', 'double'),
 ('average_amount_more_than_5000_3d', 'boolean'),
 ('average_amount_more_than_5000_5d', 'boolean'),
 ('avg_amount_change_3d_5d', 'double')]

    if "amount_features" not in globals():
        res_html += fail("`amount_features` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`amount_features` found")
    
    # ===========================================================================

    if type(globals()["amount_features"]).__name__ != "transformation":
        res_html += fail("`amount_features` is not a `transformation`")
        displayHTML(res_html)
        return

    res_html += checkmark("`amount_features` is a `transformation`")
    
    # ===========================================================================
    
    if type(amount_features.previous_decorator_instance).__name__ != "feature_decorator":
        res_html += fail("`amount_features` does NOT have a `feature` decorator")
        displayHTML(res_html)
        return

    res_html += checkmark("`amount_features` has a `feature` decorator")
    
    # ===========================================================================
    
    f_defs = len(amount_features.previous_decorator_instance._args)
    if f_defs != 3:
        res_html += fail(f"{f_defs} feature definitions is {'more' if f_defs > 3 else 'less'} than 3")
        displayHTML(res_html)
        return
    
    res_html += checkmark("there is one and only one feature definition")
    
    # ===========================================================================
    
    name_temps = {arg.name_template for arg in amount_features.previous_decorator_instance._args}
    description_temps = {arg.description_template for arg in amount_features.previous_decorator_instance._args}
    if name_temps != {"average_amount_more_than_5000_{time_window}", "avg_amount_{time_window}", "count_amount_{time_window}"}:
        res_html += fail("feature name templates are wrong")
        displayHTML(res_html)
        return
    
    res_html += checkmark("feature name templates are correct")
    
    # ===========================================================================
    
    if description_temps != {"Average of amount is greater than 5000 in last {time_window}", "Count of amount in last {time_window}", "Average of amount in last {time_window}"}:
        res_html += fail("feature description templates are wrong")
        displayHTML(res_html)
        return
    
    res_html += checkmark("feature description templates are correct")
    
    # ===========================================================================
    
    fillnas = {arg.name_template: arg.fillna_with for arg in amount_features.previous_decorator_instance._args}
    if fillnas["avg_amount_{time_window}"] != 0 or fillnas["count_amount_{time_window}"] != 0 or fillnas["count_amount_{time_window}"] != False:
        res_html += fail("one of the feature definition `fillna_with` arguments is wrong")
        displayHTML(res_html)
        return
    
    res_html += checkmark("feature definition `fillna_with` arguments are correct")
    
    # ===========================================================================
    
    if set(globals()["amount_features"].result.dtypes) != set(expected_dtypes):
        res_html += fail("the resulting DataFrame does NOT have the correct schema")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has the correct schema")
    
    # ===========================================================================

    count = globals()["amount_features"].result.count()
    if count != 998:
        res_html += fail(f"the resulting DataFrame number of rows {count} != 998")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has 998 rows")
    
    # ===========================================================================
    
    if amount_features_df.groupBy().agg(f.sum(f.col("avg_amount_3d").cast("integer"))).collect()[0][0] != 5373667:
        res_html += fail(f"the resulting feature is NOT calculated correctly")
        displayHTML(res_html)
        return
    
    res_html += checkmark("the resulting feature is calculated correctly")
    
    globals()["check_amount_features_pass"] = True
    displayHTML(res_html)

def final_check():
    res_html = ""

    if "check_bootstrap_pass" not in globals() or not globals()["check_bootstrap_pass"]:
        res_html += fail(f"`check_bootstrap` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_bootstrap` passed")

    if (
        "check_timestamps_pass" not in globals()
        or not globals()["check_timestamps_pass"]
    ):
        res_html += fail(f"`check_timestamps` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_timestamps` passed")

    if "check_make_windowed_pass" not in globals() or not globals()["check_make_windowed_pass"]:
        res_html += fail(f"`check_make_windowed` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_customer_transactions_with_timestamps` passed")

    if (
        "check_make_windowed_pass" not in globals()
        or not globals()["check_make_windowed"]
    ):
        res_html += fail(f"`check_make_windowed` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_make_windowed` passed")
    
    if (
        "check_make_windowed_pass" not in globals()
        or not globals()["check_make_windowed"]
    ):
        res_html += fail(f"`check_make_windowed` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_make_windowed` passed")
    
    if (
        "check_sum_features_pass" not in globals()
        or not globals()["check_sum_features_pass"]
    ):
        res_html += fail(f"`check_sum_features` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_sum_features` passed")
    
    if (
        "check_amount_features_pass" not in globals()
        or not globals()["check_amount_features_pass"]
    ):
        res_html += fail(f"`check_amount_features` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_amount_features` passed")
    
    try:
        dbutils.fs.rm(f"dbfs:/dev/odap-capstone/{db_name}", True)
        spark.sql(f"drop database if exists dev_{db_name} cascade")
    except:
        res_html += fail(f"cleanup failed")
        displayHTML(res_html)
        raise
        
    res_html += checkmark("cleanup successful")
    displayHTML(res_html)

# COMMAND ----------

from pyspark.sql import functions as f

spark.sql(f"create database if not exists dev_{db_name}")


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
