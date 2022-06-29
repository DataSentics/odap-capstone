# Databricks notebook source
import os


def checkmark(task):
    displayHTML(
        f'<img src="https://emojipedia-us.s3.amazonaws.com/source/skype/289/check-mark-button_2705.png" width="50"><h1>{task} successful</h1>'
    )


def fail(task):
    displayHTML(
        f'<img src="https://emojipedia-us.s3.amazonaws.com/source/skype/289/cross-mark_274c.png" width="50"><h1>{task} failed</h1>'
    )


def check_bootstrap():
    if os.environ["DAIPE_BOOTSTRAPPED"] is not None:
        checkmark("Bootstrap")
    else:
        fail("Bootstrap")


def check_load_customers():
    expected_dtypes = [
        ("id", "string"),
        ("first_name", "string"),
        ("last_name", "string"),
        ("email", "string"),
        ("gender", "string"),
        ("country", "string"),
        ("city", "string"),
    ]

    if (
        "load_customers" in globals()
        and type(globals()["load_customers"]).__name__ == "transformation"
        and set(globals()["load_customers"].result.dtypes) == set(expected_dtypes)
    ):
        checkmark("Load customers")
    else:
        fail("Load customers")
        

def check_join_customers_and_transactions():
    expected_tr_dtypes = [('id', 'string'),
 ('timestamp', 'string'),
 ('amount', 'string'),
 ('customer_id', 'string'),
 ('transaction_city', 'string')]
    
    expected_joined_dtypes = [('id', 'string'),
 ('first_name', 'string'),
 ('last_name', 'string'),
 ('email', 'string'),
 ('gender', 'string'),
 ('country', 'string'),
 ('city', 'string'),
 ('timestamp', 'string'),
 ('amount', 'string'),
 ('customer_id', 'string'),
 ('transaction_city', 'string')]
    
    if (
        "load_transactions" in globals()
        and type(globals()["load_transactions"]).__name__ == "transformation"
        and set(globals()["load_transactions"].result.dtypes) == set(expected_tr_dtypes)
        and "join_customers_and_transactions" in globals()
        and type(globals()["join_customers_and_transactions"]).__name__ == "transformation"
        and set(globals()["join_customers_and_transactions"].result.dtypes) == set(expected_joined_dtypes)
    ):
        checkmark("Join customers and transactions")
    else:
        fail("Join customers and transactions")

# COMMAND ----------

data_project_path = "file:///" + "/".join(os.getcwd().split("/")[:5]) + "/data"
data_source_path = "dbfs:/tmp/odap-capstone/data"

# COMMAND ----------

res = dbutils.fs.cp(data_project_path, data_source_path, True)
