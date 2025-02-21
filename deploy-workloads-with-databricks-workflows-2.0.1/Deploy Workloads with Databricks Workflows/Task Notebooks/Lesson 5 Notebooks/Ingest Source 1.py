# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-5L

# COMMAND ----------

def test_parameter_value():
    """
    Retrieves the value of the task parameter `test_value` and performs a check.
    
    Returns:
        True - If the value is Succeed
        False - If the value is not equal to Succeed
    """ 

    ## Create a widget from the test_value task parameter
    dbutils.widgets.text(name='test_value', defaultValue='')

    ## Get the value from the task parameter. If the parameter is Succeed, return a True value. Otherwise return false.
    if dbutils.widgets.get('test_value') == "Succeed":
        return True
    return False


## The assert statement will return an error if the value is False
assert test_parameter_value() == True

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
