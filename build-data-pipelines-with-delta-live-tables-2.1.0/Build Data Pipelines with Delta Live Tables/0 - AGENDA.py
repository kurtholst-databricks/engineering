# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Data Pipelines with Delta Live Tables
# MAGIC
# MAGIC
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the Data Engineer Learning Path by Databricks Academy.
# MAGIC | # | Module Name |
# MAGIC | --- | --- |
# MAGIC | 1 | [a - Using the DLT UI - PART 1 - Orders]($./1a - Using the DLT UI - PART 1 - Orders) |
# MAGIC | 2 | [b - Using the DLT UI - PART 2 - Customers]($./1b - Using the DLT UI - PART 2 - Customers) |
# MAGIC | 3 | [c - Using the DLT UI - PART 3 Lab - Status]($./1c - Using the DLT UI - PART 3 Lab - Status) |
# MAGIC | 4 | Orders Pipeline: [SQL]($./2A - SQL Pipelines/1 - Orders Pipeline) or [Python]($./2B - Python Pipelines/1 - Orders Pipeline)|
# MAGIC | 5 | Customers Pipeline: [SQL]($./2A - SQL Pipelines/2 - Customers Pipeline) or [Python]($./2B - Python Pipelines/2 - Customers Pipeline) |
# MAGIC | 6 | Status Pipeline Lab: [SQL]($./2A - SQL Pipelines/3L - Status Pipeline Lab) or [Python]($./2B - Python Pipelines/3L - Status Pipeline Lab) |
# MAGIC | 7 | [Delta Live Tables Running Modes]($./3 -  Delta Live Tables Running Modes) |
# MAGIC | 8 | [Pipeline Results]($./4 - Pipeline Results) |
# MAGIC | 9 | [Pipeline Event Logs]($./5 - Pipeline Event Logs) |
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use the following Databricks runtime: **15.4.x-scala2.12**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
