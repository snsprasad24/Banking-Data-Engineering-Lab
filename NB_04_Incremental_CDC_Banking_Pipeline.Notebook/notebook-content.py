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

# Step 2A — Create the Day 1 source data

from pyspark.sql import Row

day1_data = [
    Row(Transaction_ID="TXN001", Customer_ID="C001", Amount=500.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN002", Customer_ID="C002", Amount=750.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN003", Customer_ID="C003", Amount=300.00, Status="PENDING")
]

df_day1 = spark.createDataFrame(day1_data)

display(df_day1)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 2B — Create Day 2 CDC data
day2_data = [
    Row(Transaction_ID="TXN003", Customer_ID="C003", Amount=300.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN004", Customer_ID="C004", Amount=1200.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN005", Customer_ID="C005", Amount=450.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN004", Customer_ID="C004", Amount=1200.00, Status="COMPLETED")
]

df_day2 = spark.createDataFrame(day2_data)

display(df_day2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 2C — Detect duplicates
from pyspark.sql.functions import count

df_day2.groupBy("Transaction_ID") \
    .agg(count("*").alias("record_count")) \
    .filter("record_count > 1") \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 2D — Remove duplicates before MERGE
df_day2_dedup = df_day2.dropDuplicates(["Transaction_ID"])

display(df_day2_dedup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 3 — Create the Bronze Delta table/Now we'll persist Day 1 as our initial target state.

bronze_path = "Tables/bronze_banking_transactions"

df_day1.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.read.format("delta").load(bronze_path)

display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, bronze_path)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#3Step 4A — Run the MERGE
target.alias("target").merge(
    df_day2_dedup.alias("source"),
    "target.Transaction_ID = source.Transaction_ID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("MERGE completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

bronze_path = "Tables/bronze_banking_transactions"

target = DeltaTable.forPath(spark, bronze_path)

print("DeltaTable loaded successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

day2_data = [
    Row(Transaction_ID="TXN003", Customer_ID="C003", Amount=300.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN004", Customer_ID="C004", Amount=1200.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN005", Customer_ID="C005", Amount=450.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN004", Customer_ID="C004", Amount=1200.00, Status="COMPLETED")
]

df_day2 = spark.createDataFrame(day2_data)

df_day2_dedup = df_day2.dropDuplicates(["Transaction_ID"])

display(df_day2_dedup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 4D — Validate the target
df_final = spark.read.format("delta").load(bronze_path)

display(
    df_final.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 5A — Re-run the same MERGE
target.alias("target").merge(
    df_day2_dedup.alias("source"),
    "target.Transaction_ID = source.Transaction_ID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("Retry MERGE completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 5A — Recreate target
from delta.tables import DeltaTable

bronze_path = "Tables/bronze_banking_transactions"

target = DeltaTable.forPath(spark, bronze_path)

print("DeltaTable target recreated successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 5A — Recreate df_day2_dedup
from pyspark.sql import Row

day2_data = [
    Row(Transaction_ID="TXN003", Customer_ID="C003", Amount=300.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN004", Customer_ID="C004", Amount=1200.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN005", Customer_ID="C005", Amount=450.00, Status="COMPLETED"),
    Row(Transaction_ID="TXN004", Customer_ID="C004", Amount=1200.00, Status="COMPLETED")
]

df_day2 = spark.createDataFrame(day2_data)

df_day2_dedup = df_day2.dropDuplicates(["Transaction_ID"])

display(df_day2_dedup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 5D — Validate the retry
df_after_retry = spark.read.format("delta").load(bronze_path)

print("Total rows:", df_after_retry.count())

display(
    df_after_retry.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 6A — Create a new CDC event

from pyspark.sql import Row
from datetime import datetime

cdc_change = [
    Row(
        Transaction_ID="TXN004",
        Customer_ID="C004",
        Amount=1500.00,
        Status="COMPLETED",
        Change_Timestamp=datetime(2026, 7, 29, 15, 30, 0)
    )
]

df_cdc_change = spark.createDataFrame(cdc_change)

display(df_cdc_change)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 6B — Compare the existing target with the new CDC event
#Before changing anything, let's inspect what the target currently has for TXN004.
df_current_txn = spark.read.format("delta").load(bronze_path) \
    .filter("Transaction_ID = 'TXN004'")

display(df_current_txn)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 6C — Add the timestamp to the existing target
#Our current Delta table doesn't have Change_Timestamp, so let's add that column.

spark.sql(f"""
ALTER TABLE delta.`{bronze_path}`
ADD COLUMNS (Change_Timestamp TIMESTAMP)
""")

print("Change_Timestamp column added")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 6D — Set the initial timestamp
spark.sql(f"""
UPDATE delta.`{bronze_path}`
SET Change_Timestamp = TIMESTAMP '2026-07-28 10:00:00'
WHERE Change_Timestamp IS NULL
""")

print("Initial Change_Timestamp populated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.read.format("delta")
    .load(bronze_path)
    .filter("Transaction_ID = 'TXN004'")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 6E — Make MERGE timestamp-aware

from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, bronze_path)

target.alias("target").merge(
    df_cdc_change.alias("source"),
    "target.Transaction_ID = source.Transaction_ID"
).whenMatchedUpdate(
    condition="source.Change_Timestamp > target.Change_Timestamp",
    set={
        "Customer_ID": "source.Customer_ID",
        "Amount": "source.Amount",
        "Status": "source.Status",
        "Change_Timestamp": "source.Change_Timestamp"
    }
).whenNotMatchedInsert(
    values={
        "Transaction_ID": "source.Transaction_ID",
        "Customer_ID": "source.Customer_ID",
        "Amount": "source.Amount",
        "Status": "source.Status",
        "Change_Timestamp": "source.Change_Timestamp"
    }
).execute()

print("Timestamp-aware CDC MERGE completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 6F — Validate TXN004
display(
    spark.read.format("delta")
    .load(bronze_path)
    .filter("Transaction_ID = 'TXN004'")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 7A — Create the late-arriving event
from pyspark.sql import Row
from datetime import datetime

late_event = [
    Row(
        Transaction_ID="TXN004",
        Customer_ID="C004",
        Amount=1300.00,
        Status="COMPLETED",
        Change_Timestamp=datetime(2026, 7, 29, 12, 0, 0)
    )
]

df_late_event = spark.createDataFrame(late_event)

display(df_late_event)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 7B — Apply the timestamp-aware MERGE
# We'll use the same protection logic from Step 6.

from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, bronze_path)

target.alias("target").merge(
    df_late_event.alias("source"),
    "target.Transaction_ID = source.Transaction_ID"
).whenMatchedUpdate(
    condition="source.Change_Timestamp > target.Change_Timestamp",
    set={
        "Customer_ID": "source.Customer_ID",
        "Amount": "source.Amount",
        "Status": "source.Status",
        "Change_Timestamp": "source.Change_Timestamp"
    }
).whenNotMatchedInsert(
    values={
        "Transaction_ID": "source.Transaction_ID",
        "Customer_ID": "source.Customer_ID",
        "Amount": "source.Amount",
        "Status": "source.Status",
        "Change_Timestamp": "source.Change_Timestamp"
    }
).execute()

print("Late-arriving CDC MERGE completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_path = "Tables/bronze_banking_transactions"

print("bronze_path restored:", bronze_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 7C — Validate TXN004
display(
    spark.read.format("delta")
    .load(bronze_path)
    .filter("Transaction_ID = 'TXN004'")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 8A — Create the history table

from pyspark.sql import Row
from datetime import datetime

cdc_history_data = [
    Row(
        Transaction_ID="TXN004",
        Customer_ID="C004",
        Amount=1500.00,
        Status="COMPLETED",
        Change_Timestamp=datetime(2026, 7, 29, 15, 30, 0)
    ),
    Row(
        Transaction_ID="TXN004",
        Customer_ID="C004",
        Amount=1300.00,
        Status="COMPLETED",
        Change_Timestamp=datetime(2026, 7, 29, 12, 0, 0)
    )
]

df_cdc_history = spark.createDataFrame(cdc_history_data)

display(
    df_cdc_history.orderBy("Change_Timestamp")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 8B — Persist CDC History as Delta

cdc_history_path = "Tables/bronze_cdc_transaction_history"

df_cdc_history.write \
    .format("delta") \
    .mode("overwrite") \
    .save(cdc_history_path)

print("CDC history Delta table created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 8C — Verify the persisted history

df_history_check = (
    spark.read
    .format("delta")
    .load(cdc_history_path)
    .orderBy("Transaction_ID", "Change_Timestamp")
)

display(df_history_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 8D — Derive the latest state from history

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = (
    Window
    .partitionBy("Transaction_ID")
    .orderBy(col("Change_Timestamp").desc())
)

df_latest_from_history = (
    df_history_check
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

display(
    df_latest_from_history.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 9 — Handle Same-Timestamp CDC Events
# Step 9A — Create same-timestamp events
from pyspark.sql import Row
from datetime import datetime

same_timestamp_data = [
    Row(
        Transaction_ID="TXN006",
        Customer_ID="C006",
        Amount=500.00,
        Status="PENDING",
        Change_Timestamp=datetime(2026, 7, 29, 16, 0, 0),
        Event_Sequence=101
    ),
    Row(
        Transaction_ID="TXN006",
        Customer_ID="C006",
        Amount=550.00,
        Status="COMPLETED",
        Change_Timestamp=datetime(2026, 7, 29, 16, 0, 0),
        Event_Sequence=102
    )
]

df_same_timestamp = spark.createDataFrame(same_timestamp_data)

display(df_same_timestamp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 9B — Pick the deterministic latest record

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = (
    Window
    .partitionBy("Transaction_ID")
    .orderBy(
        col("Change_Timestamp").desc(),
        col("Event_Sequence").desc()
    )
)

df_latest_same_timestamp = (
    df_same_timestamp
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

display(df_latest_same_timestamp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 9C — Persist Sequence-Aware CDC History
#First, let's check the existing history schema
df_history_check.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 9D — Add Event_Sequence

spark.sql(f"""
ALTER TABLE delta.`{cdc_history_path}`
ADD COLUMNS (Event_Sequence BIGINT)
""")

print("Event_Sequence column added to CDC history")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 9E — Append both TXN006 events
#Now we'll append, not overwrite, the two events to the history Delta table.

df_same_timestamp.write \
    .format("delta") \
    .mode("append") \
    .save(cdc_history_path)

print("TXN006 CDC events appended to history")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 9F — Verify TXN006 history
df_txn006_history = (
    spark.read
    .format("delta")
    .load(cdc_history_path)
    .filter("Transaction_ID = 'TXN006'")
    .orderBy("Change_Timestamp", "Event_Sequence")
)

display(df_txn006_history)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 10 — Build the Current-State Table from CDC History
#Step 10A — Build the Current-State DataFrame
cdc_history_path = "Tables/bronze_cdc_transaction_history"

df_cdc_history = (
    spark.read
    .format("delta")
    .load(cdc_history_path)
)

print("CDC history loaded successfully")
display(
    df_cdc_history.orderBy("Transaction_ID", "Change_Timestamp", "Event_Sequence")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = (
    Window
    .partitionBy("Transaction_ID")
    .orderBy(
        col("Change_Timestamp").desc(),
        col("Event_Sequence").desc()
    )
)

df_current_state = (
    df_cdc_history
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

display(
    df_current_state.orderBy("Transaction_ID")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 10C — Persist the Current-State table
current_state_path = "Tables/silver_banking_transaction_current_state"

df_current_state.write \
    .format("delta") \
    .mode("overwrite") \
    .save(current_state_path)

print("Current-state Delta table created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 10D — Verify the persisted Silver table
#Let's make sure the table survives independently of the notebook DataFrame
df_current_state_check = (
    spark.read
    .format("delta")
    .load(current_state_path)
    .orderBy("Transaction_ID")
)

display(df_current_state_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Now we'll test a new CDC event arriving after our current-state table already exists.
from pyspark.sql import Row
from datetime import datetime

new_cdc_data = [
    Row(
        Transaction_ID="TXN004",
        Customer_ID="C004",
        Amount=1800.00,
        Status="COMPLETED",
        Change_Timestamp=datetime(2026, 7, 29, 17, 0, 0),
        Event_Sequence=103
    )
]

df_new_cdc = spark.createDataFrame(new_cdc_data)

display(df_new_cdc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 11B — Append the new event to CDC History
df_new_cdc.write \
    .format("delta") \
    .mode("append") \
    .save(cdc_history_path)

print("New CDC event appended to history")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 11C — Verify TXN004 CDC History
df_txn004_history = (
    spark.read
    .format("delta")
    .load(cdc_history_path)
    .filter("Transaction_ID = 'TXN004'")
    .orderBy("Change_Timestamp", "Event_Sequence")
)

display(df_txn004_history)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 11D — Rebuild the Current State
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = (
    Window
    .partitionBy("Transaction_ID")
    .orderBy(
        col("Change_Timestamp").desc(),
        col("Event_Sequence").desc()
    )
)

df_current_state_v2 = (
    df_cdc_history
    .unionByName(df_new_cdc)
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

display(
    df_current_state_v2
    .filter("Transaction_ID = 'TXN004'")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 11E — Update Silver Current State
df_current_state_v2.write \
    .format("delta") \
    .mode("overwrite") \
    .save(current_state_path)

print("Silver current-state table refreshed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 11F — Verify Silver Current State
df_silver_check = (
    spark.read
    .format("delta")
    .load(current_state_path)
    .filter("Transaction_ID = 'TXN004'")
)

display(df_silver_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 12 — Replay / Duplicate Event Test 🔄

df_replay = spark.createDataFrame([
    (
        "TXN004",
        "C004",
        1800.00,
        "COMPLETED",
        "2026-07-29 17:00:00",
        103
    )
], [
    "Transaction_ID",
    "Customer_ID",
    "Amount",
    "Status",
    "Change_Timestamp",
    "Event_Sequence"
])

df_replay = df_replay.withColumn(
    "Change_Timestamp",
    col("Change_Timestamp").cast("timestamp")
)

display(df_replay)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 12B — MERGE the Replay
from delta.tables import DeltaTable

silver_target = DeltaTable.forPath(spark, current_state_path)

(
    silver_target.alias("target")
    .merge(
        df_replay.alias("source"),
        "target.Transaction_ID = source.Transaction_ID"
    )
    .whenMatchedUpdate(
        condition="""
            source.Change_Timestamp > target.Change_Timestamp
            OR (
                source.Change_Timestamp = target.Change_Timestamp
                AND source.Event_Sequence > target.Event_Sequence
            )
        """,
        set={
            "Customer_ID": "source.Customer_ID",
            "Amount": "source.Amount",
            "Status": "source.Status",
            "Change_Timestamp": "source.Change_Timestamp",
            "Event_Sequence": "source.Event_Sequence"
        }
    )
    .whenNotMatchedInsert(
        values={
            "Transaction_ID": "source.Transaction_ID",
            "Customer_ID": "source.Customer_ID",
            "Amount": "source.Amount",
            "Status": "source.Status",
            "Change_Timestamp": "source.Change_Timestamp",
            "Event_Sequence": "source.Event_Sequence"
        }
    )
    .execute()
)

print("Replay MERGE completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 12C — Validate Idempotency
df_replay_check = (
    spark.read
    .format("delta")
    .load(current_state_path)
    .filter("Transaction_ID = 'TXN004'")
)

display(df_replay_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 13 — Replay an Older CDC Event 
#Now we'll test a more dangerous replay scenario
#Step 13A — Create the Older Replay

df_old_replay = spark.createDataFrame([
    (
        "TXN004",
        "C004",
        1500.00,
        "COMPLETED",
        "2026-07-29 15:30:00",
        None
    )
], [
    "Transaction_ID",
    "Customer_ID",
    "Amount",
    "Status",
    "Change_Timestamp",
    "Event_Sequence"
])

df_old_replay = df_old_replay.withColumn(
    "Change_Timestamp",
    col("Change_Timestamp").cast("timestamp")
)

display(df_old_replay)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 13A — Fixed Version
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    TimestampType, LongType
)
from datetime import datetime

schema = StructType([
    StructField("Transaction_ID", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Status", StringType(), True),
    StructField("Change_Timestamp", TimestampType(), True),
    StructField("Event_Sequence", LongType(), True)
])

df_old_replay = spark.createDataFrame([
    (
        "TXN004",
        "C004",
        1500.00,
        "COMPLETED",
        datetime(2026, 7, 29, 15, 30, 0),
        None
    )
], schema)

display(df_old_replay)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 13B — MERGE the Older Replay

silver_target = DeltaTable.forPath(spark, current_state_path)

(
    silver_target.alias("target")
    .merge(
        df_old_replay.alias("source"),
        "target.Transaction_ID = source.Transaction_ID"
    )
    .whenMatchedUpdate(
        condition="""
            source.Change_Timestamp > target.Change_Timestamp
            OR (
                source.Change_Timestamp = target.Change_Timestamp
                AND COALESCE(source.Event_Sequence, -1)
                    > COALESCE(target.Event_Sequence, -1)
            )
        """,
        set={
            "Customer_ID": "source.Customer_ID",
            "Amount": "source.Amount",
            "Status": "source.Status",
            "Change_Timestamp": "source.Change_Timestamp",
            "Event_Sequence": "source.Event_Sequence"
        }
    )
    .whenNotMatchedInsert(
        values={
            "Transaction_ID": "source.Transaction_ID",
            "Customer_ID": "source.Customer_ID",
            "Amount": "source.Amount",
            "Status": "source.Status",
            "Change_Timestamp": "source.Change_Timestamp",
            "Event_Sequence": "source.Event_Sequence"
        }
    )
    .execute()
)

print("Older replay MERGE completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 13C — Validate Current State
df_old_replay_check = (
    spark.read
    .format("delta")
    .load(current_state_path)
    .filter("Transaction_ID = 'TXN004'")
)

display(df_old_replay_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 14 — Multi-event CDC Batch /Step 14A — Create the CDC Batch
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

cdc_batch_data = [
    (
        "TXN007",
        "C007",
        700.00,
        "COMPLETED",
        datetime(2026, 7, 29, 18, 0, 0),
        201
    ),
    (
        "TXN004",
        "C004",
        2000.00,
        "COMPLETED",
        datetime(2026, 7, 29, 18, 0, 0),
        105
    ),
    (
        "TXN006",
        "C006",
        500.00,
        "PENDING",
        datetime(2026, 7, 29, 15, 0, 0),
        100
    ),
    (
        "TXN008",
        "C008",
        800.00,
        "PENDING",
        datetime(2026, 7, 29, 18, 5, 0),
        202
    )
]

df_cdc_batch = spark.createDataFrame(
    cdc_batch_data,
    batch_schema
)

display(
    df_cdc_batch.orderBy("Transaction_ID")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 14B — Deduplicate the incoming batch

batch_window = (
    Window
    .partitionBy("Transaction_ID")
    .orderBy(
        col("Change_Timestamp").desc(),
        col("Event_Sequence").desc()
    )
)

df_cdc_batch_dedup = (
    df_cdc_batch
    .withColumn("rn", row_number().over(batch_window))
    .filter(col("rn") == 1)
    .drop("rn")
)

display(
    df_cdc_batch_dedup.orderBy("Transaction_ID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 14C — MERGE the whole batch into Silver
silver_target = DeltaTable.forPath(spark, current_state_path)

(
    silver_target.alias("target")
    .merge(
        df_cdc_batch_dedup.alias("source"),
        "target.Transaction_ID = source.Transaction_ID"
    )
    .whenMatchedUpdate(
        condition="""
            source.Change_Timestamp > target.Change_Timestamp
            OR (
                source.Change_Timestamp = target.Change_Timestamp
                AND COALESCE(source.Event_Sequence, -1)
                    > COALESCE(target.Event_Sequence, -1)
            )
        """,
        set={
            "Customer_ID": "source.Customer_ID",
            "Amount": "source.Amount",
            "Status": "source.Status",
            "Change_Timestamp": "source.Change_Timestamp",
            "Event_Sequence": "source.Event_Sequence"
        }
    )
    .whenNotMatchedInsert(
        values={
            "Transaction_ID": "source.Transaction_ID",
            "Customer_ID": "source.Customer_ID",
            "Amount": "source.Amount",
            "Status": "source.Status",
            "Change_Timestamp": "source.Change_Timestamp",
            "Event_Sequence": "source.Event_Sequence"
        }
    )
    .execute()
)

print("Multi-event CDC MERGE completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Step 14D — Validate all four transactions
df_batch_result = (
    spark.read
    .format("delta")
    .load(current_state_path)
    .filter(col("Transaction_ID").isin(
        "TXN004", "TXN006", "TXN007", "TXN008"
    ))
    .orderBy("Transaction_ID")
)

display(df_batch_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Next: Step 15 — Duplicate records inside the same batch


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
