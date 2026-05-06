# Pipeline Flow

## Purpose

This document describes the end-to-end pipeline flow for the Telco CDR Lakehouse Modernization project.

## High-Level Flow

```text
Generate simulated CDR batches
    -> Register batch metadata
        -> Load raw records into Bronze
            -> Apply quality rules
                -> Write valid records to Silver Clean
                -> Write invalid records to Silver Quarantine
                    -> Build Gold aggregates
                        -> Build reconciliation results
                            -> Query with Databricks SQL
```

## Step 1 - Generate CDR Batches

The project generates synthetic CDR data to simulate incoming telecom source files.

Example source systems:

- MEDIATION_A
- MEDIATION_B
- ROAMING_PARTNER

Each batch includes:

- batch_id
- source_system
- source_file
- arrival_ts
- expected_record_count
- generated CDR records

## Step 2 - Register Metadata

Each generated batch is registered in:

```text
metadata_ingestion_log
```

This table allows the pipeline to compare expected and actual record counts later.

## Step 3 - Bronze Ingestion

Raw CDR records are written to:

```text
bronze_cdr_raw
```

The Bronze table keeps raw values and ingestion metadata.

No strict business validation is applied at this stage.

## Step 4 - Silver Quality Transformation

The pipeline reads from Bronze and applies data quality rules:

- Timestamp validation
- Duration validation
- Event type validation
- Subscriber MSISDN validation
- Charge amount validation
- Duplicate detection

Valid records are written to:

```text
silver_cdr_clean
```

Invalid records are written to:

```text
silver_cdr_quarantine
```

## Step 5 - Gold Aggregations

The pipeline creates analytics-ready tables:

```text
gold_cdr_usage_hourly
gold_subscriber_usage_daily
```

These tables support usage analysis, event type distribution, subscriber-level reporting, and dashboard metrics.

## Step 6 - Reconciliation

The pipeline compares:

```text
expected_record_count
bronze_record_count
silver_valid_count
quarantine_count
duplicate_count
```

Results are written to:

```text
gold_reconciliation_daily
```

The main validation rule is:

```text
bronze_record_count = silver_valid_count + quarantine_count
```

## Step 7 - SQL Analytics

Databricks SQL queries are used to analyze:

- Total CDR events
- Valid vs quarantined records
- Event type distribution
- Hourly usage trend
- Top subscribers by charge
- Reconciliation status by batch
- Reject reasons breakdown

## Pipeline Outcome

At the end of the pipeline, the project produces:

- Raw preserved data
- Clean validated data
- Quarantined invalid data
- Usage aggregate tables
- Reconciliation outputs
- Dashboard-ready SQL queries

## Production Notes

In production, this pipeline could be scheduled and monitored with:

- Databricks Jobs
- Cloud Composer / Apache Airflow
- Azure Data Factory
- Google Workflows
- AWS Step Functions

The batch simulation step would be replaced by real file ingestion from GCS, S3, or ADLS.
