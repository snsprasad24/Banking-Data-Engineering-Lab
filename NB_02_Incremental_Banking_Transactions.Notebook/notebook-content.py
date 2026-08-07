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

from pyspark.sql.functions import max as spark_max

max_silver_id = (
    spark.table("silver_transactions")
    .agg(spark_max("transaction_id"))
    .collect()[0][0]
)

print(max_silver_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

new_file_path = (
    "Files/Bronze/banking/transactions/"
    "transactions_20260726.csv"
)

df_new = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(new_file_path)
)

display(df_new)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Before append:", spark.table("bronze_transactions").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_new.write
    .format("delta")
    .mode("append")
    .saveAsTable("bronze_transactions")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_file_path = "Files/Bronze/banking/transactions/transactions_20260726.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(new_file_path)
)

display(df_new)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Before append:", spark.table("bronze_transactions").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_new.write
    .format("delta")
    .mode("append")
    .saveAsTable("bronze_transactions")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("bronze_transactions").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_new_fixed = (
    df_new
    .withColumn("amount", col("amount").cast("double"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_fixed.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_new_fixed.write
    .format("delta")
    .mode("append")
    .saveAsTable("bronze_transactions")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Bronze row count:",
    spark.table("bronze_transactions").count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

max_silver_id = (
    spark.table("silver_transactions")
    .agg({"transaction_id": "max"})
    .collect()[0][0]
)

print("Last processed transaction ID:", max_silver_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_bronze = (
    spark.table("bronze_transactions")
    .filter(col("transaction_id") > max_silver_id)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, current_timestamp

df_new_silver = (
    df_new_bronze
    .withColumn("transaction_id", col("transaction_id").cast("int"))
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("status", col("status").cast("string"))
    .withColumn("transaction_date", col("transaction_date").cast("timestamp"))
    .withColumn("updated_at", col("updated_at").cast("timestamp"))
    .withColumn("processed_at", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_transactions").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_new_silver_fixed = (
    df_new_silver
    .withColumn(
        "transaction_id",
        col("transaction_id").cast("long")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_fixed.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_new_silver_fixed.write
    .format("delta")
    .mode("append")
    .saveAsTable("silver_transactions")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_transactions").printSchema()
df_new_silver_fixed.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_new_silver_final = (
    df_new_silver_fixed
    .withColumn("customer_id", col("customer_id").cast("long"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_new_silver_final.write
    .format("delta")
    .mode("append")
    .saveAsTable("silver_transactions")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_transactions").printSchema()
df_new_silver_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final = (
    df_new_silver_final
    .withColumn("amount", col("amount").cast("double"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_new_silver_final.write
    .format("delta")
    .mode("append")
    .saveAsTable("silver_transactions")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_schema = spark.table("silver_transactions").schema
incoming_schema = df_new_silver_final.schema

print("SILVER TARGET:")
for field in silver_schema:
    print(field.name, field.dataType)

print("\nINCOMING:")
for field in incoming_schema:
    print(field.name, field.dataType)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType

df_new_silver_final = (
    df_new_silver_final
    .withColumn(
        "amount",
        col("amount").cast(DecimalType(18, 2))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final = (
    df_new_silver_final
    .withColumn(
        "amount",
        col("amount").cast("double")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(spark.table("silver_transactions").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_transactions").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final = (
    df_new_silver
    .withColumn("transaction_id", col("transaction_id").cast("long"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn(
        "amount",
        col("amount").cast("decimal(18,2)")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_transactions").printSchema()
df_new_silver_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## The safest next step is to align the incoming DataFrame's column order and types with the existing table:
silver_columns = spark.table("silver_transactions").columns

df_new_silver_final = df_new_silver_final.select(silver_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_new_silver_final.write
    .format("delta")
    .mode("append")
    .saveAsTable("silver_transactions")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Silver row count:",
    spark.table("silver_transactions").count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count

display(
    spark.table("silver_transactions")
    .groupBy("transaction_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_deduped = (
    spark.table("silver_transactions")
    .dropDuplicates(["transaction_id"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    df_silver_deduped.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_transactions")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Silver row count:",
    spark.table("silver_transactions").count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DecimalType

df_new_bronze = (
    spark.table("bronze_transactions")
    .filter(col("transaction_id") > 1005)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Transform it to the Silver schema
df_new_silver_final = (
    df_new_bronze
    .withColumn("transaction_id", col("transaction_id").cast("long"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn(
        "amount",
        col("amount").cast(DecimalType(18, 2))
    )
    .withColumn("status", col("status").cast("string"))
    .withColumn(
        "transaction_date",
        col("transaction_date").cast("timestamp")
    )
    .withColumn(
        "updated_at",
        col("updated_at").cast("timestamp")
    )
    .withColumn("processed_at", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_silver_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Create the temporary view
df_new_silver_final.createOrReplaceTempView(
    "new_silver_transactions"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_transactions").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DecimalType

df_new_bronze = (
    spark.table("bronze_transactions")
    .filter(col("transaction_id") > 1005)
)

display(df_new_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final = (
    df_new_bronze
    .withColumn("transaction_id", col("transaction_id").cast("long"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn(
        "amount",
        col("amount").cast(DecimalType(18, 2))
    )
    .withColumn("status", col("status").cast("string"))
    .withColumn(
        "transaction_date",
        col("transaction_date").cast("timestamp")
    )
    .withColumn(
        "updated_at",
        col("updated_at").cast("timestamp")
    )
    .withColumn("processed_at", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_silver_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final.createOrReplaceTempView(
    "new_silver_transactions"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DecimalType

df_new_bronze = (
    spark.table("bronze_transactions")
    .filter(col("transaction_id") > 1005)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final = (
    df_new_bronze
    .withColumn("transaction_id", col("transaction_id").cast("long"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn("amount", col("amount").cast(DecimalType(18, 2)))
    .withColumn("status", col("status").cast("string"))
    .withColumn("transaction_date", col("transaction_date").cast("timestamp"))
    .withColumn("updated_at", col("updated_at").cast("timestamp"))
    .withColumn("processed_at", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final.createOrReplaceTempView(
    "new_silver_transactions"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
MERGE INTO silver_transactions AS target
USING new_silver_transactions AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN UPDATE SET *

WHEN NOT MATCHED THEN INSERT *
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_transactions").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## looking for created temp view table.
spark.catalog.listTables()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## directly also can check
spark.catalog.tableExists("new_silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


## to see temp view  data 

display(
    spark.sql("""
        SELECT *
        FROM new_silver_transactions
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DecimalType

df_new_bronze = (
    spark.table("bronze_transactions")
    .filter(col("transaction_id") > 1005)
)

display(df_new_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final = (
    df_new_bronze
    .withColumn("transaction_id", col("transaction_id").cast("long"))
    .withColumn("customer_id", col("customer_id").cast("long"))
    .withColumn(
        "amount",
        col("amount").cast(DecimalType(18, 2))
    )
    .withColumn("status", col("status").cast("string"))
    .withColumn(
        "transaction_date",
        col("transaction_date").cast("timestamp")
    )
    .withColumn(
        "updated_at",
        col("updated_at").cast("timestamp")
    )
    .withColumn("processed_at", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_silver_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_silver_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## direct DataFrame-to-table MERGE
from delta.tables import DeltaTable

target = DeltaTable.forName(
    spark,
    "silver_transactions"
)

(
    target.alias("target")
    .merge(
        df_new_silver_final.alias("source"),
        "target.transaction_id = source.transaction_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_transactions").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Step 1 — Read the new Silver transactions
## Because the session timed out, recreate the DataFrame from the persistent Silver table:


from pyspark.sql.functions import col

df_new_silver = (
    spark.table("silver_transactions")
  ##  .filter(col("transaction_id").isin(1006, 1007))
       .filter(col("transaction_id") > max_silver_id)
)

display(df_new_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Step 2 — Aggregate them for Gold

from pyspark.sql.functions import (
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min
)

df_new_gold = (
    df_new_silver
    .groupBy("transaction_date")
    .agg(
        count("*").alias("total_transactions"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("average_transaction_amount"),
        spark_max("amount").alias("max_transaction_amount"),
        spark_min("amount").alias("min_transaction_amount")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_gold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Step 3 — Check the existing Gold schema
##Before merging, run:

spark.table(
    "gold_daily_transaction_summary"
).printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_gold.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_new_silver = (
    spark.table("silver_transactions")
    ##.filter(col("transaction_id").isin(1006, 1007))
    .filter(col("transaction_id") > max_silver_id)
)

display(df_new_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min
)

df_new_gold = (
    df_new_silver
    .groupBy("transaction_date")
    .agg(
        count("*").alias("total_transactions"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("average_transaction_amount"),
        spark_max("amount").alias("max_transaction_amount"),
        spark_min("amount").alias("min_transaction_amount")
    )
)

display(df_new_gold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table(
    "gold_daily_transaction_summary"
).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_gold.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Fix the incoming Gold DataFrame
##Convert transaction_date from timestamp to date and rename it to transaction_day:

from pyspark.sql.functions import col

df_new_gold_final = (
    df_new_gold
    .withColumn(
        "transaction_day",
        col("transaction_date").cast("date")
    )
    .drop("transaction_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_gold_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table(
    "gold_daily_transaction_summary"
).printSchema()

df_new_gold_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table(
    "gold_daily_transaction_summary"
).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Recreate the input Silver DataFrame
from pyspark.sql.functions import col

df_new_silver = (
    spark.table("silver_transactions")
   ## .filter(col("transaction_id").isin(1006, 1007))
    .filter(col("transaction_id") > max_silver_id)
)

display(df_new_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##. Recreate the Gold aggregation
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
    when
)

df_new_gold = (
    df_new_silver
    .groupBy("transaction_date")
    .agg(
        count("*").alias("total_transactions"),

        spark_sum(
            when(col("status") == "SUCCESS", 1)
            .otherwise(0)
        ).alias("successful_transactions"),

        spark_sum("amount").alias("total_amount"),

        avg("amount").alias(
            "average_transaction_amount"
        ),

        spark_max("amount").alias(
            "max_transaction_amount"
        ),

        spark_min("amount").alias(
            "min_transaction_amount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Align the date column
df_new_gold_final = (
    df_new_gold
    .withColumn(
        "transaction_day",
        col("transaction_date").cast("date")
    )
    .drop("transaction_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Verify the source schema
df_new_gold_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_gold_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##we should aggregate these rows again before the MERGE:
from pyspark.sql.functions import (
    sum as spark_sum,
    max as spark_max,
    min as spark_min,
    avg,
    col
)

df_new_gold_daily = (
    df_new_gold_final
    .groupBy("transaction_day")
    .agg(
        spark_sum("total_transactions").alias("total_transactions"),
        spark_sum("successful_transactions").alias("successful_transactions"),
        spark_sum("total_amount").alias("total_amount"),
        avg("average_transaction_amount").alias(
            "average_transaction_amount"
        ),
        spark_max("max_transaction_amount").alias(
            "max_transaction_amount"
        ),
        spark_min("min_transaction_amount").alias(
            "min_transaction_amount"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_gold_daily)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_gold_daily.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##incoming Gold DataFrame is correctly prepared
##Your source has exactly one row per transaction_day:
## And the schema is compatible with the Gold table:
##Now run the Gold MERGE

from delta.tables import DeltaTable

gold_target = DeltaTable.forName(
    spark,
    "gold_daily_transaction_summary"
)

(
    gold_target.alias("target")
    .merge(
        df_new_gold_daily.alias("source"),
        "target.transaction_day = source.transaction_day"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##add failed_transactions
##failed_transactions = total_transactions - successful_transactions, Create a new DataFrame:
from pyspark.sql.functions import col

df_new_gold_final = (
    df_new_gold_daily
    .withColumn(
        "failed_transactions",
        col("total_transactions") -
        col("successful_transactions")
    )
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_new_gold_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_gold_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Gold MERGE
from delta.tables import DeltaTable

gold_target = DeltaTable.forName(
    spark,
    "gold_daily_transaction_summary"
)

(
    gold_target.alias("target")
    .merge(
        df_new_gold_final.alias("source"),
        "target.transaction_day = source.transaction_day"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
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
    ).orderBy("transaction_day")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table(
    "gold_daily_transaction_summary"
).filter(
    "transaction_day = '2026-07-26'"
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Bronze:", spark.table("bronze_transactions").count())
print("Silver:", spark.table("silver_transactions").count())
print("Gold:", spark.table("gold_daily_transaction_summary").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
