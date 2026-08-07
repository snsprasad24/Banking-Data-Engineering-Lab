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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/Bronze/banking/transactions/transactions_20260725.csv")

display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("bronze_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("bronze_transactions"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col,
    to_timestamp,
    upper,
    trim,
    current_timestamp
)

df_silver = (
    df
    .withColumn("transaction_id", col("transaction_id").cast("long"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn("amount", col("amount").cast("decimal(18,2)"))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn(
        "transaction_date",
        to_timestamp(col("transaction_date"))
    )
    .withColumn(
        "updated_at",
        to_timestamp(col("updated_at"))
    )
    .withColumn(
        "processed_at",
        current_timestamp()
    )
)

display(df_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("silver_transactions"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_silver.select(
        "transaction_id",
        "customer_id",
        "amount"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_silver
    .select("status")
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

upper(trim(col("status")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.select(
    "transaction_date",
    "updated_at",
    "processed_at"
).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_silver.select(
        "transaction_id",
        "transaction_date",
        "updated_at",
        "processed_at"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum,
    avg,
    when
)

df_gold = (
    df_silver
    .withColumn(
        "transaction_day",
        to_date(col("transaction_date"))
    )
    .groupBy("transaction_day")
    .agg(
        count("transaction_id").alias("total_transactions"),

        sum("amount").alias("total_amount"),

        avg("amount").alias(
            "average_transaction_amount"
        ),

        sum(
            when(
                col("status") == "SUCCESS",
                1
            ).otherwise(0)
        ).alias("successful_transactions"),

        sum(
            when(
                col("status") == "FAILED",
                1
            ).otherwise(0)
        ).alias("failed_transactions")
    )
)

display(df_gold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "gold_daily_transaction_summary"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table(
        "gold_daily_transaction_summary"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("gold_daily_transaction_summary")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
