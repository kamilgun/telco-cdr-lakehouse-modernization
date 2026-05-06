# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

bronze_df = spark.table("bronze_cdr_raw")

display(bronze_df.limit(10))

# COMMAND ----------

typed_df = (
    bronze_df
    .withColumn(
        "event_ts",
        F.expr("try_to_timestamp(start_time_raw, 'yyyy-MM-dd HH:mm:ss')")
    )
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn("event_hour", F.hour("event_ts"))
    .withColumn(
        "duration_sec",
        F.expr("try_cast(duration_raw as int)")
    )
    .withColumn(
        "charge_amount",
        F.expr("try_cast(charge_amount_raw as double)")
    )
    .withColumn(
        "roaming_flag_bool",
        F.when(F.col("roaming_flag") == "Y", F.lit(True))
         .when(F.col("roaming_flag") == "N", F.lit(False))
         .otherwise(F.lit(None))
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("source_system"),
                F.col("cdr_id"),
                F.col("subscriber_msisdn"),
                F.col("other_party_msisdn"),
                F.col("start_time_raw")
            ),
            256
        )
    )
    .withColumn("processed_ts", F.current_timestamp())
)

display(typed_df.limit(10))

# COMMAND ----------

duplicate_window = Window.partitionBy("source_system", "cdr_id").orderBy(F.col("ingestion_ts").asc())

dedup_df = (
    typed_df
    .withColumn("row_number_in_key", F.row_number().over(duplicate_window))
    .withColumn(
        "is_duplicate",
        F.when(F.col("row_number_in_key") > 1, F.lit(True)).otherwise(F.lit(False))
    )
)

display(
    dedup_df
    .filter(F.col("is_duplicate") == True)
    .select("batch_id", "source_system", "source_file", "cdr_id", "is_duplicate")
    .limit(20)
)

# COMMAND ----------

valid_event_types = ["VOICE", "SMS", "DATA", "ROAMING"]

quality_df = (
    dedup_df
    .withColumn(
        "reject_reason",
        F.when(
            F.col("subscriber_msisdn").isNull() | (F.trim(F.col("subscriber_msisdn")) == ""),
            F.lit("MISSING_MSISDN")
        )
        .when(
            F.col("event_ts").isNull(),
            F.lit("INVALID_TIMESTAMP")
        )
        .when(
            F.col("duration_sec").isNull() | (F.col("duration_sec") <= 0),
            F.lit("INVALID_DURATION")
        )
        .when(
            F.col("charge_amount").isNull() | (F.col("charge_amount") < 0),
            F.lit("NEGATIVE_CHARGE_AMOUNT")
        )
        .when(
            ~F.col("event_type").isin(valid_event_types),
            F.lit("UNKNOWN_EVENT_TYPE")
        )
        .when(
            F.col("is_duplicate") == True,
            F.lit("DUPLICATE_CDR_ID")
        )
        .otherwise(F.lit(None))
    )
    .withColumn(
        "quality_status",
        F.when(F.col("reject_reason").isNull(), F.lit("VALID")).otherwise(F.lit("REJECTED"))
    )
)

display(
    quality_df
    .groupBy("quality_status", "reject_reason")
    .count()
    .orderBy("quality_status", "reject_reason")
)

# COMMAND ----------

silver_clean_df = (
    quality_df
    .filter(F.col("quality_status") == "VALID")
    .select(
        "batch_id",
        "source_system",
        "source_file",
        "cdr_id",
        "subscriber_msisdn",
        "other_party_msisdn",
        "event_type",
        "event_ts",
        "event_date",
        "event_hour",
        "duration_sec",
        "cell_id",
        "switch_id",
        F.col("roaming_flag_bool").alias("roaming_flag"),
        "charge_amount",
        "currency",
        "record_hash",
        "quality_status",
        "is_duplicate",
        "processed_ts"
    )
)

display(silver_clean_df.limit(10))

# COMMAND ----------

silver_quarantine_df = (
    quality_df
    .filter(F.col("quality_status") == "REJECTED")
    .select(
        "batch_id",
        "source_system",
        "source_file",
        "cdr_id",
        "subscriber_msisdn",
        "event_type",
        "start_time_raw",
        "duration_raw",
        "charge_amount_raw",
        "reject_reason",
        F.current_timestamp().alias("reject_ts")
    )
)

display(silver_quarantine_df.limit(20))

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver_cdr_clean")
spark.sql("DROP TABLE IF EXISTS silver_cdr_quarantine")

silver_clean_df.write.mode("overwrite").saveAsTable("silver_cdr_clean")
silver_quarantine_df.write.mode("overwrite").saveAsTable("silver_cdr_quarantine")

# COMMAND ----------

bronze_count = spark.table("bronze_cdr_raw").count()
silver_clean_count = spark.table("silver_cdr_clean").count()
silver_quarantine_count = spark.table("silver_cdr_quarantine").count()

print(f"Bronze count: {bronze_count}")
print(f"Silver clean count: {silver_clean_count}")
print(f"Silver quarantine count: {silver_quarantine_count}")
print(f"Check: bronze = clean + quarantine ? {bronze_count == silver_clean_count + silver_quarantine_count}")

# COMMAND ----------

display(
    spark.table("silver_cdr_quarantine")
    .groupBy("reject_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(
    quality_df
    .groupBy("source_system", "quality_status")
    .count()
    .orderBy("source_system", "quality_status")
)