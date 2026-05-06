# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver_df = spark.table("silver_cdr_clean")

print(f"Silver clean count: {silver_df.count()}")

display(silver_df.limit(10))

# COMMAND ----------

gold_usage_hourly_df = (
    silver_df
    .groupBy(
        "event_date",
        "event_hour",
        "source_system",
        "event_type"
    )
    .agg(
        F.count("*").alias("total_events"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.sum("charge_amount"), 2).alias("total_charge"),
        F.countDistinct("subscriber_msisdn").alias("unique_subscribers"),
        F.sum(
            F.when(F.col("roaming_flag") == True, F.lit(1)).otherwise(F.lit(0))
        ).alias("roaming_events")
    )
    .withColumnRenamed("event_date", "usage_date").withColumnRenamed("event_hour", "usage_hour")
)

display(
    gold_usage_hourly_df
    .orderBy("usage_date", "usage_hour", "source_system", "event_type")
)

# COMMAND ----------

gold_subscriber_daily_df = (
    silver_df
    .groupBy(
        "event_date",
        "subscriber_msisdn"
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "VOICE", 1).otherwise(0)).alias("voice_call_count"),
        F.sum(F.when(F.col("event_type") == "SMS", 1).otherwise(0)).alias("sms_count"),
        F.sum(F.when(F.col("event_type") == "DATA", 1).otherwise(0)).alias("data_session_count"),
        F.sum(F.when(F.col("event_type") == "ROAMING", 1).otherwise(0)).alias("roaming_event_count"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.sum("charge_amount"), 2).alias("total_charge")
    )
    .withColumnRenamed("event_date", "usage_date")
)

display(
    gold_subscriber_daily_df
    .orderBy(F.desc("total_charge"))
    .limit(20)
)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gold_cdr_usage_hourly")
spark.sql("DROP TABLE IF EXISTS gold_subscriber_usage_daily")

gold_usage_hourly_df.write.mode("overwrite").saveAsTable("gold_cdr_usage_hourly")
gold_subscriber_daily_df.write.mode("overwrite").saveAsTable("gold_subscriber_usage_daily")

# COMMAND ----------

print("Gold hourly usage:")
display(
    spark.table("gold_cdr_usage_hourly")
    .orderBy("usage_date", "usage_hour", "source_system", "event_type")
    .limit(20)
)

print("Gold subscriber daily:")
display(
    spark.table("gold_subscriber_usage_daily")
    .orderBy(F.desc("total_charge"))
    .limit(20)
)

# COMMAND ----------

summary_df = spark.createDataFrame(
    [
        ("Silver Clean Records", spark.table("silver_cdr_clean").count()),
        ("Gold Hourly Rows", spark.table("gold_cdr_usage_hourly").count()),
        ("Gold Subscriber Daily Rows", spark.table("gold_subscriber_usage_daily").count())
    ],
    ["metric", "value"]
)

display(summary_df)