# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a94e46e5-56c1-4db5-8108-cd0110ef7a78",
# META       "default_lakehouse_name": "LH_Banking_Data",
# META       "default_lakehouse_workspace_id": "bfb763ef-437a-4fa3-bfaf-2e4090d3f39c",
# META       "known_lakehouses": [
# META         {
# META           "id": "a94e46e5-56c1-4db5-8108-cd0110ef7a78"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************


pPipelineName = "PL_Banking_ETL"
pActivityName = ""
pRunId = ""
pErrorMessage = ""
pProcessDate = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO etl_audit_log
# MAGIC (
# MAGIC     pipeline_name,
# MAGIC     activity_name,
# MAGIC     run_id,
# MAGIC     error_message,
# MAGIC     process_date,
# MAGIC     audit_timestamp
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC     '${pPipelineName}',
# MAGIC     '${pActivityName}',
# MAGIC     '${pRunId}',
# MAGIC     '${pErrorMessage}',
# MAGIC     CAST(NULLIF('${pProcessDate}', '') AS DATE),
# MAGIC     current_timestamp()
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE etl_audit_log

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE etl_audit_log
# MAGIC (
# MAGIC     pipeline_name STRING,
# MAGIC     activity_name STRING,
# MAGIC     run_id STRING,
# MAGIC     error_message STRING,
# MAGIC     process_date DATE,
# MAGIC     audit_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SHOW TABLES

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
