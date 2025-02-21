-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Pipeline Events Logs
-- MAGIC
-- MAGIC DLT uses the event logs to store much of the important information used to manage, report, and understand what's happening during pipeline execution.
-- MAGIC
-- MAGIC DLT stores log information in a special table called the **`event_log`**. To access log data from this table, you must use the **`event_log`** table valued function (TVF). We will use this function in the cells that follow, passing the  pipeline id as a parameter.
-- MAGIC
-- MAGIC Below, we provide a number of useful queries to explore the event log and gain greater insight into your DLT pipelines.
-- MAGIC
-- MAGIC For more information view the [Monitor Delta Live Tables pipelines](https://docs.databricks.com/en/delta-live-tables/observability.html#monitor-delta-live-tables-pipelines) documentation.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED! - You must have a DLT pipeline created
-- MAGIC - If you have not completed the DLT pipeline from the previous steps (**1a, 1b, and 1c**), open the notebook **4 - Pipeline Results** and create the DLT pipeline for this course by running cells 5 & 7 using a classic compute cluster. Wait a few minutes for the DLT pipeline to complete execution.
-- MAGIC
-- MAGIC - If you have not completed demo **3 - Delta Live Tables Running Modes**, your numbers might not match, but you can still continue with the demonstration.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED! - Please Change to SQL Warehouse!
-- MAGIC The rest of the cells in this notebook must be run on a SQL warehouse.
-- MAGIC
-- MAGIC **TO DO:** Please select the compute drop down at the top right of the page and select the **your-user-name SQL 2X-Small** SQL warehouse prior to running the cells in this notebook. If you do not select **Shared Warehouse** you will not be able to complete this demo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A1. Query the DLT Event Log
-- MAGIC The Delta Live Tables event log contains all information related to a pipeline, including audit logs, data quality checks, pipeline progress, and data lineage. You can use the event log to track, understand, and monitor the state of your data pipelines.
-- MAGIC
-- MAGIC The event log is managed as a Delta Lake table with some of the more important fields stored as nested JSON data.
-- MAGIC
-- MAGIC **REQUIRED:** Copy the pipeline id and run the code. This establishes a temporary view into the pipeline event log, showing how simple it is to read the event log table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Complete the following to get the DLT pipeline id.
-- MAGIC
-- MAGIC 1. Right click on the **Delta Live Tables** tab in the left sidebar and select *Open in new tab*.
-- MAGIC
-- MAGIC 1. Locate the pipeline you created in the previous notebooks:
-- MAGIC    * Click on the pipeline.
-- MAGIC    * In the Pipeline Details tab on the right sidebar, find the `Pipeline Id`.
-- MAGIC    * Copy the Pipeline Id and paste it in the cell below.
-- MAGIC
-- MAGIC ![GetPipelineID](files/images/build-data-pipelines-with-delta-live-tables-2.1.0/GetPipelineID.png)

-- COMMAND ----------

-- This cell must be run on a shared SQL warehouse.
CREATE OR REPLACE TEMPORARY VIEW pipeline_event_log AS 
SELECT * FROM event_log('FILL-PIPELINE-ID');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's query the view to see the contents of the event log table.

-- COMMAND ----------

SELECT * 
FROM pipeline_event_log

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The query in the previous cell uses the [**`event_log`** table-valued function](https://docs.databricks.com/en/sql/language-manual/functions/event_log.html). This is a built in function that allows you to query the event log for materialized views, streaming tables, and DLT pipelines.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A2. Perform Audit Logging
-- MAGIC
-- MAGIC Events related to running pipelines and editing configurations are captured as **`user_action`**.
-- MAGIC
-- MAGIC Yours should be the only **`user_name`** for the pipeline you configured during this lesson.

-- COMMAND ----------

SELECT 
  timestamp, 
  details:user_action:action, 
  details:user_action:user_name
FROM pipeline_event_log
WHERE event_type = 'user_action'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A3. Get Latest Update ID
-- MAGIC
-- MAGIC In many cases, you may wish to get information about the latest update to your pipeline.
-- MAGIC
-- MAGIC We can easily capture the most recent update ID with a SQL query.

-- COMMAND ----------

-- Create an SQL STRING variable named latest_updated_id
DECLARE OR REPLACE VARIABLE latest_update_id STRING;

-- Populate the new SQL variable with the latest update ID
SET VARIABLE latest_update_id = (
    SELECT origin.update_id
    FROM pipeline_event_log
    WHERE event_type = 'create_update'
    ORDER BY timestamp DESC LIMIT 1
);

-- View the value of latest_update_id
SELECT latest_update_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A4. Examine Lineage
-- MAGIC
-- MAGIC DLT provides built-in lineage information for how data flows through your table.
-- MAGIC
-- MAGIC While the query below only indicates the direct predecessors for each table, this information can easily be combined to trace data in any table back to the point it entered the lakehouse.

-- COMMAND ----------

SELECT 
  details:flow_definition.output_dataset, 
  details:flow_definition.input_datasets 
FROM pipeline_event_log
WHERE event_type = 'flow_definition' AND 
      origin.update_id = latest_update_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A5. Examine Data Quality Metrics
-- MAGIC
-- MAGIC Finally, data quality metrics can be extremely useful for both long term and short term insights into your data.
-- MAGIC
-- MAGIC Below, we capture the metrics for each constraint throughout the entire lifetime of our table.

-- COMMAND ----------

SELECT 
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as failing_records
FROM
  (SELECT explode(
            from_json(details :flow_progress :data_quality :expectations,
                      "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
          ) row_expectations
   FROM pipeline_event_log
   WHERE event_type = 'flow_progress' AND 
         origin.update_id = latest_update_id
  )
GROUP BY row_expectations.dataset, row_expectations.name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
