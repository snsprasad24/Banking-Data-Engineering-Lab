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

# MARKDOWN ********************

# NB_06_Performance_Optimization
# Goal
# 
# A Senior Data Engineer spends a significant amount of time improving performance rather than just writing ETL logic.
# 
# In this notebook we'll optimize:
# 
# Reading Delta tables
# Writing Delta tables
# Partitioning
# Filtering
# File sizes
# Caching
# Broadcast joins
# Shuffle reduction
# Adaptive Query Execution
# Monitoring execution plans

# MARKDOWN ********************

# Step 1 — Check Current Spark Configuration
# 
# First, let's see what Spark settings are currently enabled in your Fabric notebook

# CELL ********************

spark.conf.getAll()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("AQE:", spark.conf.get("spark.sql.adaptive.enabled"))
print("Shuffle Partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
print("Broadcast Threshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
print("Max Partition Bytes:", spark.conf.get("spark.sql.files.maxPartitionBytes"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.explain(True)
gold_df.explain("extended")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = spark.read.table("gold_transactions")   # or your actual Gold table name
gold_df.explain(True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Step 2A – List All Tables in Your Lakehouse

# CELL ********************

spark.sql("SHOW TABLES").show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.catalog.listTables())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in spark.catalog.listTables():
    print(table.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# NB_06 – Step 2B: Analyze the Gold Table Execution Plan

# CELL ********************

gold_df = spark.read.table("gold_daily_transaction_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.explain("extended")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Once we review your execution plan, we'll move into the practical optimization topics:
# 
# ✅ Execution plan analysis
# Partition pruning
# Filter pushdown
# Broadcast joins
# Shuffle reduction
# Caching
# Repartition vs. Coalesce
# Delta optimization (OPTIMIZE/Z-Ordering where supported)
# Performance benchmarking

# CELL ********************

gold_df = spark.read.table("gold_daily_transaction_summary")

gold_df.explain("extended")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# NB_06 – Step 3: Demonstrate Predicate Pushdown
# 
# Now let's make Spark actually optimize something.

# CELL ********************

gold_df.filter(gold_df.transaction_day == "2026-08-01").explain("extended")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# NB_06 – Step 4: Performance Benchmarking
# 
# Before we optimize joins, caching, or partitioning, let's establish a baseline.

# CELL ********************

import time

start = time.time()

gold_df.count()

end = time.time()

print(f"Execution Time: {end - start:.3f} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# NB_06 – Step 5: Cache vs. No Cache (Production Performance)
# 
# This is one of the most frequently asked Spark interview topics.

# MARKDOWN ********************

# Why Caching?
# 
# Imagine this workflow:
# 
# Silver Table
#       │
#       ▼
# Data Cleaning
#       │
#       ▼
# Business Rules
#       │
#       ▼
# Aggregation
#       │
#       ├── Dashboard
#       ├── ML Model
#       ├── Audit Report
#       └── Validation
# 
# If Spark doesn't cache the DataFrame, each action recomputes the entire lineage.
# 
# With caching:
# 
# Silver Table
#       │
#       ▼
# Transformations
#       │
#       ▼
# Memory Cache
#       │
#  ┌────┼────┐
#  ▼    ▼    ▼
# Count Show Write
# 
# The expensive transformations are executed once and reused.

# MARKDOWN ********************

# Step 5A – Cache the Gold DataFrame

# CELL ********************

gold_df.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Why call count()?
# Spark uses lazy evaluation. Calling cache() only marks the DataFrame for caching. An action such as count() actually loads it into memory.

# MARKDOWN ********************

# Step 5B – Measure Again

# CELL ********************

import time

start = time.time()

gold_df.count()

end = time.time()

print(f"Cached Execution Time: {end - start:.3f} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df.cache()

gold_df.count()

import time

start = time.time()

gold_df.count()

end = time.time()

print(f"Cached Execution Time: {end - start:.3f} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# NB_06 – Step 6: Broadcast Join Optimization ⭐⭐⭐⭐⭐
# 
# This is one of the most important Spark optimization techniques and a very common interview topic.

# MARKDOWN ********************

# Step 6A – Load Bronze and Silver Tables

# CELL ********************

bronze_df = spark.read.table("bronze_transactions")
silver_df = spark.read.table("silver_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Bronze:", bronze_df.count())
print("Silver:", silver_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Step 6B – Create a Small Dimension Table
# We'll create a small lookup table from the Silver data to simulate a dimension table:

# CELL ********************

customer_dim = (
    silver_df
    .select("Customer_ID")
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_dim.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Step 6C – Force a Broadcast Join

# CELL ********************

from pyspark.sql.functions import broadcast

joined_df = bronze_df.join(
    broadcast(customer_dim),
    "Customer_ID"
)

joined_df.explain("extended")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Next Step: Step 7 – Repartition vs. Coalesce
# 
# In this step, we'll learn:
# 
# Why too many small files hurt performance.
# The difference between repartition() and coalesce().
# How to choose the right number of partitions.
# How partitioning affects parallelism and job execution.
# Real production scenarios for Bronze, Silver, and Gold layers.

# CELL ********************

#Step 7A – Check Current Number of Partitions
print("Bronze Partitions:", bronze_df.rdd.getNumPartitions())

print("Silver Partitions:", silver_df.rdd.getNumPartitions())

print("Gold Partitions:", gold_df.rdd.getNumPartitions())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_repart = bronze_df.repartition(8)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Coalesce()

# CELL ********************

gold_small = gold_df.coalesce(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Step 7B – Let's Test It`

# CELL ********************

print("Original:", bronze_df.rdd.getNumPartitions())

bronze_repart = bronze_df.repartition(8)

print("After Repartition:", bronze_repart.rdd.getNumPartitions())

bronze_coalesce = bronze_repart.coalesce(2)

print("After Coalesce:", bronze_coalesce.rdd.getNumPartitions())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Step 8 – Small File Optimization

# CELL ********************

#Step 8A – Check Your Current File Layout
gold_df.inputFiles()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(gold_df.inputFiles())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Number of files:", len(gold_df.inputFiles()))

for file in gold_df.inputFiles():
    print(file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Step 1 – Detect Skew in Our Bronze Table

# CELL ********************

from pyspark.sql.functions import count

bronze_df.groupBy("status") \
    .agg(count("*").alias("record_count")) \
    .orderBy("record_count", ascending=False) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df.groupBy("customer_id") \
    .agg(count("*").alias("transactions")) \
    .orderBy("transactions", ascending=False) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
