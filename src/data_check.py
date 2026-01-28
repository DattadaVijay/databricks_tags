# Databricks notebook source

pip install datacontract-cli[databricks] pyarrow

# COMMAND ----------
from datacontract.data_contract import DataContract
data_contract = DataContract(data_contract_file="../fixtures/odcs.yaml")
run = data_contract.test()
if not run.has_passed():
    print("Data quality validation failed.")