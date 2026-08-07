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

#Step 15B — Create the failure-test batch

from datetime import datetime

failure_test_data = [
    ("TXN010", "C010", 1000.00, "COMPLETED", datetime(2026, 7, 30, 10, 0, 0), 301),
    ("TXN011", "C011", 1250.00, "COMPLETED", datetime(2026, 7, 30, 10, 5, 0), 302),
    ("TXN012", "C012", 800.00, "PENDING", datetime(2026, 7, 30, 10, 10, 0), 303)
]

df_failure_test = spark.createDataFrame(
    failure_test_data,
    batch_schema
)

display(df_failure_test)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 15B — Define the schema
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    TimestampType, LongType
)
from datetime import datetime

batch_schema = StructType([
    StructField("Transaction_ID", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Status", StringType(), True),
    StructField("Change_Timestamp", TimestampType(), True),
    StructField("Event_Sequence", LongType(), True)
])

print("Failure test schema created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 15C — Create the failure-test batch
failure_test_data = [
    ("TXN010", "C010", 1000.00, "COMPLETED", datetime(2026, 7, 30, 10, 0, 0), 301),
    ("TXN011", "C011", 1250.00, "COMPLETED", datetime(2026, 7, 30, 10, 5, 0), 302),
    ("TXN012", "C012", 800.00, "PENDING", datetime(2026, 7, 30, 10, 10, 0), 303)
]

df_failure_test = spark.createDataFrame(
    failure_test_data,
    batch_schema
)

display(df_failure_test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 15D — Write the batch to Bronze
failure_bronze_path = "Tables/bronze_failure_test"

(
    df_failure_test.write
    .format("delta")
    .mode("overwrite")
    .save(failure_bronze_path)
)

print("Failure test batch written to Bronze successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 15E — Simulate Silver Failure
print("Starting Silver processing...")

raise Exception(
    "SIMULATED_FAILURE: Silver transformation failed after Bronze ingestion"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 15F — Restart from Bronze
df_restart = (
    spark.read
    .format("delta")
    .load(failure_bronze_path)
)

print("Bronze data reloaded successfully")
display(df_restart)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 15G — Verify the recovery data
print("Recovered row count:", df_restart.count())

display(
    df_restart.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 15H — Restart Silver processing
failure_silver_path = "Tables/silver_failure_restart_test"

(
    df_restart.write
    .format("delta")
    .mode("overwrite")
    .save(failure_silver_path)
)

print("Silver restart processing completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 15I — Verify the restarted Silver table
df_restart_silver = (
    spark.read
    .format("delta")
    .load(failure_silver_path)
)

print("Silver row count:", df_restart_silver.count())

display(
    df_restart_silver.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 15J — Run the same restart again
(
    df_restart.write
    .format("delta")
    .mode("overwrite")
    .save(failure_silver_path)
)

print("Retry restart completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_retry_check = (
    spark.read
    .format("delta")
    .load(failure_silver_path)
)

print("Row count after retry:", df_retry_check.count())

display(
    df_retry_check.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 16 — Backfill & Reprocessing
from datetime import datetime

backfill_data = [
    ("TXN020", "C020", 2000.00, "COMPLETED",
     datetime(2026, 7, 28, 9, 0, 0), 401),

    ("TXN021", "C021", 1500.00, "COMPLETED",
     datetime(2026, 7, 28, 10, 0, 0), 402),

    ("TXN022", "C022", 900.00, "PENDING",
     datetime(2026, 7, 28, 11, 0, 0), 403)
]

df_backfill = spark.createDataFrame(
    backfill_data,
    batch_schema
)

display(df_backfill)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 16B — Write the backfill data to a separate Bronze table
backfill_bronze_path = "Tables/bronze_backfill_test"

(
    df_backfill.write
    .format("delta")
    .mode("overwrite")
    .save(backfill_bronze_path)
)

print("Backfill data written to Bronze successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 16C — Reprocess the backfill
df_backfill_restart = (
    spark.read
    .format("delta")
    .load(backfill_bronze_path)
)

backfill_silver_path = "Tables/silver_backfill_test"

(
    df_backfill_restart.write
    .format("delta")
    .mode("overwrite")
    .save(backfill_silver_path)
)

print("Backfill reprocessing completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 16D — Validate backfilled data
df_backfill_result = (
    spark.read
    .format("delta")
    .load(backfill_silver_path)
)

print("Backfill row count:", df_backfill_result.count())

display(
    df_backfill_result.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 17: Audit & Reconciliation
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    TimestampType
)

audit_schema = StructType([
    StructField("Pipeline_Name", StringType(), True),
    StructField("Run_ID", StringType(), True),
    StructField("Source_Count", IntegerType(), True),
    StructField("Bronze_Count", IntegerType(), True),
    StructField("Silver_Count", IntegerType(), True),
    StructField("Rejected_Count", IntegerType(), True),
    StructField("Pipeline_Status", StringType(), True),
    StructField("Audit_Timestamp", TimestampType(), True)
])

print("Audit schema created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 17B — Capture reconciliation counts
from datetime import datetime

source_count = df_backfill.count()
bronze_count = df_backfill_restart.count()
silver_count = df_backfill_result.count()

rejected_count = source_count - silver_count

audit_record = [(
    "PL_Banking_Backfill_Test",
    "RUN_001",
    source_count,
    bronze_count,
    silver_count,
    rejected_count,
    "SUCCESS",
    datetime.now()
)]

df_audit = spark.createDataFrame(
    audit_record,
    audit_schema
)

display(df_audit)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 17C — Persist the audit record
#Right now df_audit is only a DataFrame in memory. We want an actual Delta audit table.
audit_path = "Tables/etl_audit_reconciliation_test"

(
    df_audit.write
    .format("delta")
    .mode("append")
    .save(audit_path)
)

print("Audit record persisted successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 17D — Read the audit table
df_audit_check = (
    spark.read
    .format("delta")
    .load(audit_path)
)

display(
    df_audit_check.orderBy("Audit_Timestamp")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 17E — Record a failed run
#One final practical piece: audit tables should capture failures too, not just successful runs.
failure_audit_record = [(
    "PL_Banking_Backfill_Test",
    "RUN_002",
    3,
    3,
    0,
    3,
    "FAILED",
    datetime.now()
)]

df_failure_audit = spark.createDataFrame(
    failure_audit_record,
    audit_schema
)

(
    df_failure_audit.write
    .format("delta")
    .mode("append")
    .save(audit_path)
)

print("Failed run audit record persisted successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Step 17 — Audit & Reconciliation ✅ COMPLETE
# 
# You now have a persisted audit table that can answer:
# 
# How many records arrived?
# How many reached Bronze?
# How many reached Silver?
# How many were rejected?
# Did the pipeline succeed or fail?
# Which run was it?

# MARKDOWN ********************

# 🚀 Next: Step 18 — Data Quality
# 
# We'll keep this one focused on the few checks that matter most in real projects:
# 
# Required fields / NULL checks
# Duplicate business keys
# Invalid amounts
# Invalid status values
# Record-count/reconciliation checks
# 
# Then we'll route bad records separately instead of allowing them silently into Silver.
# 
# No huge data-quality framework — just the 80/20 essentials.
# We'll test the 4 checks that matter most:
# 
# NULL required fields
# Duplicate business key
# Invalid amount
# Invalid status
# 


# CELL ********************

#We'll use a separate test dataset so your existing tables remain untouched
from datetime import datetime

dq_test_data = [
    # Valid
    ("TXN030", "C030", 1200.00, "COMPLETED",
     datetime(2026, 7, 30, 12, 0, 0), 501),

    # NULL Customer_ID
    ("TXN031", None, 800.00, "COMPLETED",
     datetime(2026, 7, 30, 12, 5, 0), 502),

    # Duplicate Transaction_ID
    ("TXN032", "C032", 500.00, "PENDING",
     datetime(2026, 7, 30, 12, 10, 0), 503),

    ("TXN032", "C032", 500.00, "PENDING",
     datetime(2026, 7, 30, 12, 10, 0), 503),

    # Invalid amount
    ("TXN033", "C033", -100.00, "COMPLETED",
     datetime(2026, 7, 30, 12, 15, 0), 504),

    # Invalid status
    ("TXN034", "C034", 900.00, "UNKNOWN",
     datetime(2026, 7, 30, 12, 20, 0), 505)
]

df_dq_test = spark.createDataFrame(
    dq_test_data,
    batch_schema
)

display(df_dq_test) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 18B — Run basic DQ checks

from pyspark.sql.functions import (
    col, count, when
)

# 1. NULL required fields
null_check = df_dq_test.filter(
    col("Transaction_ID").isNull() |
    col("Customer_ID").isNull() |
    col("Change_Timestamp").isNull()
)

# 2. Invalid amounts
invalid_amounts = df_dq_test.filter(
    col("Amount").isNull() |
    (col("Amount") < 0)
)

# 3. Invalid statuses
valid_statuses = ["PENDING", "COMPLETED", "FAILED", "CANCELLED"]

invalid_statuses = df_dq_test.filter(
    ~col("Status").isin(valid_statuses)
)

# 4. Duplicate transaction IDs
duplicate_ids = (
    df_dq_test
    .groupBy("Transaction_ID")
    .count()
    .filter(col("count") > 1)
)

print("NULL required-field rows:", null_check.count())
print("Invalid amount rows:", invalid_amounts.count())
print("Invalid status rows:", invalid_statuses.count())
print("Duplicate transaction IDs:", duplicate_ids.count())

print("\n--- NULL violations ---")
display(null_check)

print("\n--- Invalid amounts ---")
display(invalid_amounts)

print("\n--- Invalid statuses ---")
display(invalid_statuses)

print("\n--- Duplicate IDs ---")
display(duplicate_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#                  Incoming data
#                       │
#                       ▼
#                  DQ Validation
#                   /         \
#                  /           \
#              VALID          INVALID
#                │               │
#                ▼               ▼
#             Silver       Quarantine/Reject

# CELL ********************

#Step 18C — Separate valid and rejected records
from pyspark.sql.functions import col

valid_statuses = ["PENDING", "COMPLETED", "FAILED", "CANCELLED"]

valid_records = df_dq_test.filter(
    col("Transaction_ID").isNotNull() &
    col("Customer_ID").isNotNull() &
    col("Change_Timestamp").isNotNull() &
    col("Amount").isNotNull() &
    (col("Amount") >= 0) &
    col("Status").isin(valid_statuses)
)

rejected_records = df_dq_test.filter(
    col("Transaction_ID").isNull() |
    col("Customer_ID").isNull() |
    col("Change_Timestamp").isNull() |
    col("Amount").isNull() |
    (col("Amount") < 0) |
    ~col("Status").isin(valid_statuses)
)

print("Valid rows:", valid_records.count())
print("Rejected rows:", rejected_records.count())

print("\n--- VALID RECORDS ---")
display(valid_records)

print("\n--- REJECTED RECORDS ---")
display(rejected_records)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# For this lab, we'll treat Transaction_ID as the business key and keep the latest event using:
# 
# Change_Timestamp → Event_Sequence

# CELL ********************

#Step 18D — Handle duplicate business keys
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

dedup_window = Window.partitionBy(
    "Transaction_ID"
).orderBy(
    col("Change_Timestamp").desc(),
    col("Event_Sequence").desc()
)

df_deduped = (
    df_dq_test
    .withColumn("rn", row_number().over(dedup_window))
    .filter(col("rn") == 1)
    .drop("rn")
)

print("Rows after deduplication:", df_deduped.count())

display(
    df_deduped.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 18E — Final DQ validation
valid_records_final = df_deduped.filter(
    col("Transaction_ID").isNotNull() &
    col("Customer_ID").isNotNull() &
    col("Change_Timestamp").isNotNull() &
    col("Amount").isNotNull() &
    (col("Amount") >= 0) &
    col("Status").isin(valid_statuses)
)

rejected_records_final = df_deduped.filter(
    col("Transaction_ID").isNull() |
    col("Customer_ID").isNull() |
    col("Change_Timestamp").isNull() |
    col("Amount").isNull() |
    (col("Amount") < 0) |
    ~col("Status").isin(valid_statuses)
)

print("Final valid rows:", valid_records_final.count())
print("Final rejected rows:", rejected_records_final.count())

print("\n--- FINAL VALID ---")
display(valid_records_final)

print("\n--- FINAL REJECTED ---")
display(rejected_records_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 18F — Create the quarantine table
quarantine_path = "Tables/dq_quarantine_test"

(
    rejected_records_final
    .write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_path)
)

print("DQ quarantine table created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 18F — Recreate rejected records
from pyspark.sql.functions import col

valid_statuses = ["PENDING", "COMPLETED", "FAILED", "CANCELLED"]

df_dq_test = spark.createDataFrame(
    dq_test_data,
    batch_schema
)

dedup_window = Window.partitionBy(
    "Transaction_ID"
).orderBy(
    col("Change_Timestamp").desc(),
    col("Event_Sequence").desc()
)

df_deduped = (
    df_dq_test
    .withColumn("rn", row_number().over(dedup_window))
    .filter(col("rn") == 1)
    .drop("rn")
)

rejected_records_final = df_deduped.filter(
    col("Transaction_ID").isNull() |
    col("Customer_ID").isNull() |
    col("Change_Timestamp").isNull() |
    col("Amount").isNull() |
    (col("Amount") < 0) |
    ~col("Status").isin(valid_statuses)
)

print("Rejected records recreated:", rejected_records_final.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 18F — Recreate DQ test data
from datetime import datetime

dq_test_data = [
    ("TXN030", "C030", 1200.00, "COMPLETED",
     datetime(2026, 7, 30, 12, 0, 0), 501),

    ("TXN031", None, 800.00, "COMPLETED",
     datetime(2026, 7, 30, 12, 5, 0), 502),

    ("TXN032", "C032", 500.00, "PENDING",
     datetime(2026, 7, 30, 12, 10, 0), 503),

    ("TXN032", "C032", 500.00, "PENDING",
     datetime(2026, 7, 30, 12, 10, 0), 503),

    ("TXN033", "C033", -100.00, "COMPLETED",
     datetime(2026, 7, 30, 12, 15, 0), 504),

    ("TXN034", "C034", 900.00, "UNKNOWN",
     datetime(2026, 7, 30, 12, 20, 0), 505)
]

df_dq_test = spark.createDataFrame(
    dq_test_data,
    batch_schema
)

print("DQ test data recreated:", df_dq_test.count(), "rows")
display(df_dq_test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 18G — Persist DQ test batch
dq_bronze_path = "Tables/bronze_dq_test"

(
    df_dq_test.write
    .format("delta")
    .mode("overwrite")
    .save(dq_bronze_path)
)

print("DQ test batch persisted to Bronze successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 18H — Reload from Bronze and recreate DQ result

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

df_dq_source = (
    spark.read
    .format("delta")
    .load(dq_bronze_path)
)

dedup_window = Window.partitionBy(
    "Transaction_ID"
).orderBy(
    col("Change_Timestamp").desc(),
    col("Event_Sequence").desc()
)

df_deduped = (
    df_dq_source
    .withColumn("rn", row_number().over(dedup_window))
    .filter(col("rn") == 1)
    .drop("rn")
)

valid_statuses = [
    "PENDING",
    "COMPLETED",
    "FAILED",
    "CANCELLED"
]

rejected_records_final = df_deduped.filter(
    col("Transaction_ID").isNull() |
    col("Customer_ID").isNull() |
    col("Change_Timestamp").isNull() |
    col("Amount").isNull() |
    (col("Amount") < 0) |
    ~col("Status").isin(valid_statuses)
)

print("Bronze rows:", df_dq_source.count())
print("After deduplication:", df_deduped.count())
print("Rejected rows:", rejected_records_final.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 18I — Write rejected records to quarantine
quarantine_path = "Tables/dq_quarantine_test"

(
    rejected_records_final
    .write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_path)
)

print("DQ quarantine table created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 18J — Verify the quarantine records
df_quarantine_check = (
    spark.read
    .format("delta")
    .load(quarantine_path)
)

print("Quarantine row count:", df_quarantine_check.count())

display(
    df_quarantine_check.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#One final step — DQ summary
dq_summary = [(
    "DQ_TEST_RUN_001",
    df_dq_source.count(),
    df_deduped.count(),
    valid_records_final.count() if "valid_records_final" in locals() else df_deduped.filter(
        col("Transaction_ID").isNotNull() &
        col("Customer_ID").isNotNull() &
        col("Change_Timestamp").isNotNull() &
        col("Amount").isNotNull() &
        (col("Amount") >= 0) &
        col("Status").isin(valid_statuses)
    ).count(),
    rejected_records_final.count()
)]

print("DQ summary:")
print(dq_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# “I implement data-quality validation before Silver processing. I check mandatory fields, valid business values, numeric ranges, and duplicate business keys. Invalid records are quarantined for investigation rather than silently dropped, while valid records continue to Silver.”
