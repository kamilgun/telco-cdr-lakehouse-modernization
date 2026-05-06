# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import uuid

# COMMAND ----------

# Project-level configuration

SOURCE_SYSTEMS = [
    {
        "source_system": "MEDIATION_A",
        "event_focus": ["VOICE", "SMS"],
        "source_file_prefix": "cdr_mediation_a"
    },
    {
        "source_system": "MEDIATION_B",
        "event_focus": ["DATA", "VOICE"],
        "source_file_prefix": "cdr_mediation_b"
    },
    {
        "source_system": "ROAMING_PARTNER",
        "event_focus": ["ROAMING", "VOICE", "DATA"],
        "source_file_prefix": "cdr_roaming_partner"
    }
]

RECORDS_PER_BATCH = 1000
BASE_TIME = datetime.now()

# COMMAND ----------

def random_msisdn():
    return f"9053{random.randint(1000000, 9999999)}"

def random_other_party():
    return f"9054{random.randint(1000000, 9999999)}"

def create_batch_id(source_system, batch_time):
    ts = batch_time.strftime("%Y%m%d_%H%M%S")
    return f"BATCH_{ts}_{source_system}"

def create_source_file(prefix, batch_time):
    ts = batch_time.strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.csv"

# COMMAND ----------

def generate_cdr_batch(source_config, batch_time, record_count):
    source_system = source_config["source_system"]
    event_focus = source_config["event_focus"]
    source_file_prefix = source_config["source_file_prefix"]

    batch_id = create_batch_id(source_system, batch_time)
    source_file = create_source_file(source_file_prefix, batch_time)

    rows = []

    for i in range(record_count):
        cdr_id = f"{source_system}_{i}"

        event_type = random.choice(event_focus)

        event_time = batch_time - timedelta(minutes=random.randint(0, 180))
        start_time_raw = event_time.strftime("%Y-%m-%d %H:%M:%S")

        duration_raw = str(random.randint(1, 600))
        charge_amount_raw = str(round(random.uniform(0, 25), 2))

        subscriber_msisdn = random_msisdn()
        other_party_msisdn = random_other_party()

        roaming_flag = "Y" if event_type == "ROAMING" or source_system == "ROAMING_PARTNER" else random.choice(["Y", "N", "N", "N"])

        # Inject controlled data quality issues

        issue_roll = random.random()

        if issue_roll < 0.03:
            start_time_raw = "invalid_timestamp"

        elif issue_roll < 0.06:
            duration_raw = str(random.choice([0, -10, -99]))

        elif issue_roll < 0.08:
            event_type = random.choice(["UNKNOWN", "MMS", "INVALID_EVENT"])

        elif issue_roll < 0.10:
            subscriber_msisdn = None

        elif issue_roll < 0.12:
            charge_amount_raw = str(round(random.uniform(-20, -1), 2))

        rows.append({
            "batch_id": batch_id,
            "source_system": source_system,
            "source_file": source_file,
            "cdr_id": cdr_id,
            "subscriber_msisdn": subscriber_msisdn,
            "other_party_msisdn": other_party_msisdn,
            "event_type": event_type,
            "start_time_raw": start_time_raw,
            "duration_raw": duration_raw,
            "cell_id": f"CELL_{random.randint(100, 999)}",
            "switch_id": f"SW_{random.randint(1, 20)}",
            "roaming_flag": roaming_flag,
            "charge_amount_raw": charge_amount_raw,
            "currency": "TRY"
        })

    # Add a few duplicate records deliberately
    duplicate_sample = random.sample(rows, 10)
    rows.extend(duplicate_sample)

    metadata = {
        "batch_id": batch_id,
        "source_system": source_system,
        "source_file": source_file,
        "arrival_ts": batch_time,
        "expected_record_count": record_count + len(duplicate_sample),
        "actual_generated_count": len(rows),
        "bronze_count": len(rows),
        "status": "GENERATED",
        "created_ts": datetime.now()
    }

    return rows, metadata

# COMMAND ----------

all_cdr_rows = []
metadata_rows = []

for idx, source_config in enumerate(SOURCE_SYSTEMS):
    batch_time = BASE_TIME - timedelta(minutes=idx * 15)
    
    cdr_rows, metadata = generate_cdr_batch(
        source_config=source_config,
        batch_time=batch_time,
        record_count=RECORDS_PER_BATCH
    )
    
    all_cdr_rows.extend(cdr_rows)
    metadata_rows.append(metadata)

print(f"Generated CDR rows: {len(all_cdr_rows)}")
print(f"Generated metadata rows: {len(metadata_rows)}")

# COMMAND ----------

bronze_schema = StructType([
    StructField("batch_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("cdr_id", StringType(), True),
    StructField("subscriber_msisdn", StringType(), True),
    StructField("other_party_msisdn", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("start_time_raw", StringType(), True),
    StructField("duration_raw", StringType(), True),
    StructField("cell_id", StringType(), True),
    StructField("switch_id", StringType(), True),
    StructField("roaming_flag", StringType(), True),
    StructField("charge_amount_raw", StringType(), True),
    StructField("currency", StringType(), True)
])

metadata_schema = StructType([
    StructField("batch_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("arrival_ts", TimestampType(), True),
    StructField("expected_record_count", IntegerType(), True),
    StructField("actual_generated_count", IntegerType(), True),
    StructField("bronze_count", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("created_ts", TimestampType(), True)
])

bronze_df = spark.createDataFrame(all_cdr_rows, schema=bronze_schema)
metadata_df = spark.createDataFrame(metadata_rows, schema=metadata_schema)

bronze_df = bronze_df.withColumn("ingestion_ts", F.current_timestamp())

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS bronze_cdr_raw")
spark.sql("DROP TABLE IF EXISTS metadata_ingestion_log")

# COMMAND ----------

bronze_df.write.mode("overwrite").saveAsTable("bronze_cdr_raw")

metadata_df.write.mode("overwrite").saveAsTable("metadata_ingestion_log")

# COMMAND ----------

spark.sql("""
SELECT 
    source_system,
    source_file,
    COUNT(*) AS bronze_record_count
FROM bronze_cdr_raw
GROUP BY source_system, source_file
ORDER BY source_system
""").show(truncate=False)

spark.sql("""
SELECT 
    batch_id,
    source_system,
    source_file,
    expected_record_count,
    actual_generated_count,
    bronze_count,
    status
FROM metadata_ingestion_log
ORDER BY source_system
""").show(truncate=False)