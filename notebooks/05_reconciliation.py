# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

metadata_df = spark.table("metadata_ingestion_log")
bronze_df = spark.table("bronze_cdr_raw")
silver_df = spark.table("silver_cdr_clean")
quarantine_df = spark.table("silver_cdr_quarantine")

print(f"Metadata count: {metadata_df.count()}")
print(f"Bronze count: {bronze_df.count()}")
print(f"Silver clean count: {silver_df.count()}")
print(f"Quarantine count: {quarantine_df.count()}")

# COMMAND ----------

bronze_counts_df = (
    bronze_df
    .groupBy("batch_id", "source_system", "source_file")
    .agg(
        F.count("*").alias("bronze_record_count")
    )
)

display(bronze_counts_df.orderBy("source_system"))

# COMMAND ----------

silver_counts_df = (
    silver_df
    .groupBy("batch_id", "source_system", "source_file")
    .agg(
        F.count("*").alias("silver_valid_count")
    )
)

display(silver_counts_df.orderBy("source_system"))

# COMMAND ----------

quarantine_counts_df = (
    quarantine_df
    .groupBy("batch_id", "source_system", "source_file")
    .agg(
        F.count("*").alias("quarantine_count"),
        F.sum(
            F.when(F.col("reject_reason") == "DUPLICATE_CDR_ID", 1).otherwise(0)
        ).alias("duplicate_count")
    )
)

display(quarantine_counts_df.orderBy("source_system"))

# COMMAND ----------

gold_reconciliation_df = (
    metadata_df.alias("m")
    .join(
        bronze_counts_df.alias("b"),
        on=["batch_id", "source_system", "source_file"],
        how="left"
    )
    .join(
        silver_counts_df.alias("s"),
        on=["batch_id", "source_system", "source_file"],
        how="left"
    )
    .join(
        quarantine_counts_df.alias("q"),
        on=["batch_id", "source_system", "source_file"],
        how="left"
    )
    .select(
        F.col("batch_id"),
        F.col("source_system"),
        F.col("source_file"),
        F.col("expected_record_count"),
        F.coalesce(F.col("bronze_record_count"), F.lit(0)).alias("bronze_record_count"),
        F.coalesce(F.col("silver_valid_count"), F.lit(0)).alias("silver_valid_count"),
        F.coalesce(F.col("quarantine_count"), F.lit(0)).alias("quarantine_count"),
        F.coalesce(F.col("duplicate_count"), F.lit(0)).alias("duplicate_count")
    )
    .withColumn(
        "reject_rate",
        F.round(F.col("quarantine_count") / F.col("bronze_record_count"), 4)
    )
    .withColumn(
        "reconciliation_status",
        F.when(
            (F.col("expected_record_count") != F.col("bronze_record_count")) |
            (F.col("bronze_record_count") != F.col("silver_valid_count") + F.col("quarantine_count")),
            F.lit("FAILED")
        )
        .when(
            F.col("reject_rate") > 0.10,
            F.lit("WARNING")
        )
        .otherwise(F.lit("RECONCILED"))
    )
    .withColumn("reconciliation_ts", F.current_timestamp())
)

display(gold_reconciliation_df.orderBy("source_system"))

# COMMAND ----------

recon_check_df = (
    gold_reconciliation_df
    .withColumn(
        "silver_plus_quarantine",
        F.col("silver_valid_count") + F.col("quarantine_count")
    )
    .withColumn(
        "is_count_matched",
        F.col("bronze_record_count") == F.col("silver_plus_quarantine")
    )
    .select(
        "batch_id",
        "source_system",
        "expected_record_count",
        "bronze_record_count",
        "silver_valid_count",
        "quarantine_count",
        "silver_plus_quarantine",
        "is_count_matched",
        "reject_rate",
        "reconciliation_status"
    )
)

display(recon_check_df.orderBy("source_system"))

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gold_reconciliation_daily")

gold_reconciliation_df.write.mode("overwrite").saveAsTable("gold_reconciliation_daily")

# COMMAND ----------

display(
    spark.table("gold_reconciliation_daily")
    .orderBy("source_system")
)

# COMMAND ----------

display(
    spark.table("gold_reconciliation_daily")
    .groupBy("reconciliation_status")
    .agg(
        F.count("*").alias("batch_count"),
        F.round(F.avg("reject_rate"), 4).alias("avg_reject_rate")
    )
    .orderBy("reconciliation_status")
)