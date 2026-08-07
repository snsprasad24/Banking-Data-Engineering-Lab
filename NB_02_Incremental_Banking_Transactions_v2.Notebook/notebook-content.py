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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import max as spark_max

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1 — Read the Silver watermark

max_silver_id = (
    spark.table("silver_transactions")
    .agg(spark_max("transaction_id"))
    .collect()[0][0]
)

print(f"Last processed transaction_id: {max_silver_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Cell 3 — Read Only New Bronze Transactions

from pyspark.sql.functions import col

df_new_bronze = (
    spark.table("bronze_transactions")
         .filter(col("transaction_id") > max_silver_id)
)

print("New Bronze Records:", df_new_bronze.count())

display(df_new_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##  Cell 4 — Standardize the incoming Bronze schema
## In production, incoming data often has different
## data types (exactly like the issues you encountered with amount,
## transaction_id, and customer_id).

from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DecimalType

df_new_silver = (
    df_new_bronze
    .withColumn("transaction_id", col("transaction_id").cast("long"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn("amount", col("amount").cast(DecimalType(18, 2)))
    .withColumn("transaction_date", to_timestamp(col("transaction_date")))
    .withColumn("updated_at", to_timestamp(col("updated_at")))
)

df_new_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

###Cell 5 — MERGE into Silver
##Step 5.1 — Create a temporary view

df_new_silver.createOrReplaceTempView("new_silver_transactions")
##Cell 5.2 — MERGE into Silver
##Create a Spark SQL cell (or use spark.sql("""...""") in a PySpark cell if you prefer).



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE INTO silver_transactions AS target
# MAGIC USING new_silver_transactions AS source
# MAGIC ON target.transaction_id = source.transaction_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     target.customer_id = source.customer_id,
# MAGIC     target.amount = source.amount,
# MAGIC     target.status = source.status,
# MAGIC     target.transaction_date = source.transaction_date,
# MAGIC     target.updated_at = source.updated_at
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     amount,
# MAGIC     status,
# MAGIC     transaction_date,
# MAGIC     updated_at
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.transaction_id,
# MAGIC     source.customer_id,
# MAGIC     source.amount,
# MAGIC     source.status,
# MAGIC     source.transaction_date,
# MAGIC     source.updated_at
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Cell 6 — Verify the Silver MERGE
##In production, we always verify that the MERGE did what we expected.

silver_count = spark.table("silver_transactions").count()

print(f"Silver Row Count : {silver_count}")

display(
    spark.table("silver_transactions")
         .orderBy("transaction_id")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Cell 7 — Build Gold Incremental Aggregate
##Now we'll aggregate only the newly processed transactions.

from pyspark.sql.functions import (
    sum,
    count,
    avg,
    max,
    min,
    when,
    to_date,
    col
)

df_new_gold = (
    df_new_silver
    .withColumn("transaction_day", to_date(col("transaction_date")))
    .groupBy("transaction_day")
    .agg(
        count("*").alias("total_transactions"),
        sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_transactions"),
        sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_transaction_amount"),
        max("amount").alias("max_transaction_amount"),
        min("amount").alias("min_transaction_amount")
    )
)

df_new_gold.printSchema()

display(df_new_gold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Final Step — Cell 8: MERGE into Gold
##Just like Silver, we now make Gold incremental.

df_new_gold.createOrReplaceTempView("new_gold_summary")

spark.sql("""
MERGE INTO gold_daily_transaction_summary AS target
USING new_gold_summary AS source
ON target.transaction_day = source.transaction_day

WHEN MATCHED THEN
UPDATE SET
    target.total_transactions = target.total_transactions + source.total_transactions,
    target.successful_transactions = target.successful_transactions + source.successful_transactions,
    target.failed_transactions = target.failed_transactions + source.failed_transactions,
    target.total_amount = target.total_amount + source.total_amount,
    target.average_transaction_amount =
        (target.total_amount + source.total_amount) /
        (target.total_transactions + source.total_transactions),
    target.max_transaction_amount =
        greatest(target.max_transaction_amount, source.max_transaction_amount),
    target.min_transaction_amount =
        least(target.min_transaction_amount, source.min_transaction_amount)

WHEN NOT MATCHED THEN
INSERT (
    transaction_day,
    total_transactions,
    successful_transactions,
    failed_transactions,
    total_amount,
    average_transaction_amount,
    max_transaction_amount,
    min_transaction_amount
)
VALUES (
    source.transaction_day,
    source.total_transactions,
    source.successful_transactions,
    source.failed_transactions,
    source.total_amount,
    source.average_transaction_amount,
    source.max_transaction_amount,
    source.min_transaction_amount
)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("gold_daily_transaction_summary").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##%%sql
##ALTER TABLE gold_daily_transaction_summary
##ADD COLUMNS (
##    max_transaction_amount DECIMAL(18,2),
##    min_transaction_amount DECIMAL(18,2)
##);

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

spark.table("gold_daily_transaction_summary").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # One-Time Historical Backfill
# 
# **Purpose**
# 
# Populate the newly added columns:
# 
# - max_transaction_amount
# - min_transaction_amount
# 
# for historical Gold records after schema evolution.
# 
# ---
# 
# **Important**
# 
# - Run this section only **once**.
# - This is **NOT** part of the daily incremental ETL pipeline.

# CELL ********************

###One-time historical backfill

from pyspark.sql.functions import (
    count,
    sum,
    avg,
    max,
    min,
    when,
    to_date,
    col
)

df_gold_backfill = (
    spark.table("silver_transactions")
    .withColumn("transaction_day", to_date(col("transaction_date")))
    .groupBy("transaction_day")
    .agg(
        count("*").alias("total_transactions"),
        sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_transactions"),
        sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_transaction_amount"),
        max("amount").alias("max_transaction_amount"),
        min("amount").alias("min_transaction_amount")
    )
)

display(df_gold_backfill)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold_backfill.createOrReplaceTempView("gold_backfill")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
MERGE INTO gold_daily_transaction_summary AS target
USING gold_backfill AS source
ON target.transaction_day = source.transaction_day

WHEN MATCHED THEN
UPDATE SET
    target.total_transactions = source.total_transactions,
    target.successful_transactions = source.successful_transactions,
    target.failed_transactions = source.failed_transactions,
    target.total_amount = source.total_amount,
    target.average_transaction_amount = source.average_transaction_amount,
    target.max_transaction_amount = source.max_transaction_amount,
    target.min_transaction_amount = source.min_transaction_amount
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("gold_daily_transaction_summary")
         .orderBy("transaction_day")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(""" 
MERGE INTO gold_daily_transaction_summary AS target
USING new_gold_summary AS source
ON target.transaction_day = source.transaction_day

WHEN MATCHED THEN
UPDATE SET
    target.total_transactions = target.total_transactions + source.total_transactions,
    target.successful_transactions = target.successful_transactions + source.successful_transactions,
    target.failed_transactions = target.failed_transactions + source.failed_transactions,
    target.total_amount = target.total_amount + source.total_amount,
    target.average_transaction_amount =
        (target.total_amount + source.total_amount) /
        (target.total_transactions + source.total_transactions),
    target.max_transaction_amount =
        greatest(target.max_transaction_amount, source.max_transaction_amount),
    target.min_transaction_amount =
        least(target.min_transaction_amount, source.min_transaction_amount)

WHEN NOT MATCHED THEN
INSERT (
    transaction_day,
    total_transactions,
    successful_transactions,
    failed_transactions,
    total_amount,
    average_transaction_amount,
    max_transaction_amount,
    min_transaction_amount
)
VALUES (
    source.transaction_day,
    source.total_transactions,
    source.successful_transactions,
    source.failed_transactions,
    source.total_amount,
    source.average_transaction_amount,
    source.max_transaction_amount,
    source.min_transaction_amount
)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("gold_daily_transaction_summary")
    .orderBy("transaction_day")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS etl_audit_log (
# MAGIC     audit_id BIGINT,
# MAGIC     pipeline_name STRING,
# MAGIC     activity_name STRING,
# MAGIC     run_id STRING,
# MAGIC     status STRING,
# MAGIC     error_message STRING,
# MAGIC     process_date DATE,
# MAGIC     event_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
