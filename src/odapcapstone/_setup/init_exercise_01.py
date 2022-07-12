# Databricks notebook source
# pylint: skip-file
import os
import inspect
import ast
from pyspark.sql.utils import AnalysisException
import sys

import json

user = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["tags"]["user"]
db_name = "odap_academy_" + ''.join(ch for ch in user if ch.isalnum())
del user

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
        or "datalakebundle" not in sys.modules
        or "databricksbundle" not in sys.modules
        or "injecta" not in sys.modules
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


def check_load_customers():
    res_html = ""

    expected_dtypes = [
        ("id", "string"),
        ("first_name", "string"),
        ("last_name", "string"),
        ("email", "string"),
        ("gender", "string"),
        ("country", "string"),
        ("city", "string"),
    ]

    if "load_customers" not in globals():
        res_html += fail("`load_customers` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`load_customers` found")

    if type(globals()["load_customers"]).__name__ != "transformation":
        res_html += fail("`load_customers` is not a `transformation`")
        displayHTML(res_html)
        return

    res_html += checkmark("`load_customers` is a `transformation`")

    if set(globals()["load_customers"].result.dtypes) != set(expected_dtypes):
        res_html += fail("the resulting DataFrame does NOT have the correct schema")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has the correct schema")

    count = globals()["load_customers"].result.count()
    if count != 1000:
        res_html += fail(f"the resulting DataFrame number of rows {count} != 1000")
        displayHTML(res_html)
        return

    res_html += checkmark("the resulting DataFrame has 1000 rows")
    globals()["check_load_customers_pass"] = True
    displayHTML(res_html)


def check_join_customers_and_transactions():
    res_html = ""

    expected_tr_dtypes = [
        ("id", "string"),
        ("timestamp", "string"),
        ("amount", "string"),
        ("customer_id", "string"),
        ("transaction_city", "string"),
    ]

    expected_joined_dtypes = [
        ("id", "string"),
        ("first_name", "string"),
        ("last_name", "string"),
        ("email", "string"),
        ("gender", "string"),
        ("country", "string"),
        ("city", "string"),
        ("timestamp", "string"),
        ("amount", "string"),
        ("customer_id", "string"),
        ("transaction_city", "string"),
    ]

    if "load_transactions" not in globals():
        res_html += fail("`load_transactions` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`load_transactions` found")

    if type(globals()["load_transactions"]).__name__ != "transformation":
        res_html += fail("`load_transactions` is not a `transformation`")
        displayHTML(res_html)
        return

    res_html += checkmark("`load_transactions` is a `transformation`")

    if set(globals()["load_transactions"].result.dtypes) != set(expected_tr_dtypes):
        res_html += fail(
            "the `load_transactions` DataFrame does NOT have the correct schema"
        )
        displayHTML(res_html)
        return

    res_html += checkmark("the `load_transactions` DataFrame has the correct schema")

    count = globals()["load_transactions"].result.count()
    if count != 1000:
        res_html += fail(
            f"the `load_transactions` DataFrame number of rows {count} != 1000"
        )
        displayHTML(res_html)
        return

    res_html += checkmark("the `load_transactions` DataFrame has 1000 rows")

    if "join_customers_and_transactions" not in globals():
        res_html += fail("`join_customers_and_transactions` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`join_customers_and_transactions` found")

    if type(globals()["join_customers_and_transactions"]).__name__ != "transformation":
        res_html += fail("`join_customers_and_transactions` is not a `transformation`")
        displayHTML(res_html)
        return

    res_html += checkmark("`join_customers_and_transactions` is a `transformation`")

    if set(globals()["join_customers_and_transactions"].result.dtypes) != set(
        expected_joined_dtypes
    ):
        res_html += fail(
            "the `join_customers_and_transactions` DataFrame does NOT have the correct schema"
        )
        displayHTML(res_html)
        return

    res_html += checkmark(
        "the `join_customers_and_transactions` DataFrame has the correct schema"
    )

    count = globals()["join_customers_and_transactions"].result.count()
    if count != 1000:
        res_html += fail(
            f"the `join_customers_and_transactions` DataFrame number of rows {count} != 1000"
        )
        displayHTML(res_html)
        return

    res_html += checkmark(
        "the `join_customers_and_transactions` DataFrame has 1000 rows"
    )
    
    transformation_count = [type(arg).__name__ for arg in globals()["join_customers_and_transactions"]._args].count("transformation")
    
    if len(globals()["join_customers_and_transactions"]._args) < 2 or transformation_count < 2:
        res_html += fail(
            f"arguments of `join_customers_and_transactions` are not the expected transformations"
        )
        displayHTML(res_html)
        return
    
    res_html += checkmark(
        "arguments of `join_customers_and_transactions` are the expected transformations"
    )
    
    globals()["check_join_customers_and_transactions_pass"] = True
    displayHTML(res_html)


def check_database():
    res_html = ""

    if "create_database" not in globals():
        res_html += fail("`create_database` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`create_database` found")

    dbs = [db.name for db in spark.catalog.listDatabases()]
    env_db_name = os.environ["APP_ENV"] + "_" + db_name
    if env_db_name not in dbs:
        res_html += fail(f"database `{env_db_name}` does NOT exist")
        displayHTML(res_html)
        return

    res_html += checkmark(f"database `{env_db_name}` exists")

    if type(globals()["create_database"]).__name__ != "notebook_function":
        res_html += fail("`create_database` is not a `notebook_function`")
        displayHTML(res_html)
        return

    res_html += checkmark("`create_database` is a `notebook_function`")

    code = inspect.getsource(create_database.function)

    try:
        if "SparkSession" not in [
            arg.annotation.id
            for arg in ast.parse(
                inspect.getsource(globals()["create_database"].function)
            )
            .body[0]
            .args.args
        ]:
            res_html += fail(f"decorated function arguments are not properly annotated")
            displayHTML(res_html)
            return
    except AttributeError:
        res_html += fail(f"decorated function arguments are not properly annotated")
        displayHTML(res_html)
        return

    res_html += checkmark(
        "decorated function SparkSession argument is properly annotated"
    )
    globals()["check_database_pass"] = True
    displayHTML(res_html)


def check_save_customer_transactions():
    res_html = ""

    expected_joined_dtypes = [
        ("id", "string"),
        ("first_name", "string"),
        ("last_name", "string"),
        ("email", "string"),
        ("gender", "string"),
        ("country", "string"),
        ("city", "string"),
        ("timestamp", "string"),
        ("amount", "string"),
        ("customer_id", "string"),
        ("transaction_city", "string"),
    ]

    if "save_customer_transactions" not in globals():
        res_html += fail("`save_customer_transactions` not found")
        displayHTML(res_html)
        return

    res_html += checkmark("`save_customer_transactions` found")

    if type(globals()["save_customer_transactions"]).__name__ != "transformation":
        res_html += fail("`save_customer_transactions` is not a `transformation`")
        displayHTML(res_html)
        return

    res_html += checkmark("`save_customer_transactions` is a `transformation`")

    if set(globals()["save_customer_transactions"].result.dtypes) != set(
        expected_joined_dtypes
    ):
        res_html += fail(
            "the `save_customer_transactions` DataFrame does NOT have the correct schema"
        )
        displayHTML(res_html)
        return

    res_html += checkmark(
        "the `save_customer_transactions` DataFrame has the correct schema"
    )

    count = globals()["save_customer_transactions"].result.count()
    if count != 1000:
        res_html += fail(
            f"the `save_customer_transactions` DataFrame number of rows {count} != 1000"
        )
        displayHTML(res_html)
        return

    res_html += checkmark("the `save_customer_transactions` DataFrame has 1000 rows")

    env_db_name = os.environ["APP_ENV"] + "_" + db_name

    try:
        df = spark.sql(
            f"describe table extended {env_db_name}.customer_transactions"
        ).toPandas()
    except AnalysisException:
        res_html += fail(f"table `{env_db_name}.customer_transactions` does NOT exist")
        displayHTML(res_html)
        return

    res_html += checkmark(
        f"table `{env_db_name}.customer_transactions` does NOT exists"
    )

    if df.loc[20].values[1] != "delta":
        res_html += fail(
            f"table `{env_db_name}.customer_transactions` is NOT a Delta table"
        )
        displayHTML(res_html)
        return

    res_html += checkmark(
        f"table `{env_db_name}.customer_transactions` is a Delta table"
    )

    path = f"dbfs:/{os.environ['APP_ENV']}/odap-capstone/{db_name}/customer_transactions.delta"
    if df.loc[19].values[1] != path:
        res_html += fail(
            f"table `{env_db_name}.customer_transactions` is NOT in the correct path"
        )
        displayHTML(res_html)
        return

    res_html += checkmark(
        f"table `{env_db_name}.customer_transactions` is in the correct path"
    )

    if (
        globals()["save_customer_transactions"].previous_decorator_instance is None
        or type(
            globals()["save_customer_transactions"].previous_decorator_instance
        ).__name__
        != "table_overwrite"
    ):
        res_html += fail(f"incorrect saving method has been used")
        displayHTML(res_html)
        return

    res_html += checkmark("`table_overwrite` has been used correctly")

    globals()["check_save_customer_transactions_pass"] = True
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

    if (
        "check_join_customers_and_transactions_pass" not in globals()
        or not globals()["check_join_customers_and_transactions_pass"]
    ):
        res_html += fail(f"`check_join_customers_and_transactions` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_join_customers_and_transactions` passed")

    if "check_database_pass" not in globals() or not globals()["check_database_pass"]:
        res_html += fail(f"`check_database` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`check_database` passed")

    if (
        "check_save_customer_transactions_pass" not in globals()
        or not globals()["check_save_customer_transactions_pass"]
    ):
        res_html += fail(f"`save_customer_transactions` not passed")
        displayHTML(res_html)
        return

    res_html += checkmark("`save_customer_transactions` passed")
    
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

data_project_path = "file:///" + "/".join(os.getcwd().split("/")[:5]) + "/data"
data_source_path = "dbfs:/tmp/odap-capstone/data"

# COMMAND ----------

res = dbutils.fs.cp(data_project_path, data_source_path, True)
