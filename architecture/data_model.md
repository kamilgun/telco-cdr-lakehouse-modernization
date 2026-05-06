# Data Model

## Purpose

This document defines the logical data model for the Telco CDR Lakehouse Modernization project.

The project follows a layered lakehouse design:

```text
Metadata
Bronze
Silver
Gold
```

Each layer has a specific responsibility in the pipeline.

## Layer Overview

| Layer | Table | Purpose |
|---|---|---|
| Metadata | metadata_ingestion_log | Track simulated source batches |
| Bronze | bronze_cdr_raw | Store raw CDR records |
| Silver | silver_cdr_clean | Store validated CDR records |
| Silver | silver_cdr_quarantine | Store rejected CDR records |
| Gold | gold_cdr_usage_hourly | Hourly usage analytics |
| Gold | gold_subscriber_usage_daily | Subscriber-level daily usage |
| Gold | gold_reconciliation_daily | Batch-level reconciliation |

## Entity Relationship Overview

```text
metadata_ingestion_log
        |
        | batch_id
        v
bronze_cdr_raw
        |
        | valid records
        v
silver_cdr_clean
        |
        +------------------> gold_cdr_usage_hourly
        |
        +------------------> gold_subscriber_usage_daily
        |
        +------------------> gold_reconciliation_daily

bronze_cdr_raw
        |
        | invalid records
        v
silver_cdr_quarantine
        |
        +------------------> gold_reconciliation_daily
```

## 1. Metadata Table

### Table Name

```text
metadata_ingestion_log
```

### Purpose

Tracks each simulated source batch.

This table represents the operational metadata normally captured during file ingestion in a production CDR platform.

### Columns

| Column | Type | Description |
|---|---|---|
| batch_id | string | Unique batch identifier |
| source_system | string | Source system name |
| source_file | string | Simulated source file name |
| arrival_ts | timestamp | Simulated arrival timestamp |
| expected_record_count | int | Expected number of records in the batch |
| actual_generated_count | int | Number of records generated in the project |
| bronze_count | int | Number of records written to Bronze |
| status | string | Batch status |
| created_ts | timestamp | Metadata creation timestamp |

### Example Row

| batch_id | source_system | source_file | expected_record_count | status |
|---|---|---|---:|---|
| BATCH_20260421_101500_MEDIATION_A | MEDIATION_A | cdr_voice_20260421_101500.csv | 1000 | GENERATED |

---

## 2. Bronze Table

### Table Name

```text
bronze_cdr_raw
```

### Purpose

Stores raw CDR records with minimal transformation.

The Bronze layer preserves raw business data and ingestion metadata.

### Columns

| Column | Type | Description |
|---|---|---|
| batch_id | string | Batch identifier |
| source_system | string | Source system name |
| source_file | string | Simulated source file name |
| cdr_id | string | CDR identifier |
| subscriber_msisdn | string | Subscriber MSISDN |
| other_party_msisdn | string | Other party MSISDN |
| event_type | string | Raw event type |
| start_time_raw | string | Raw timestamp |
| duration_raw | string | Raw duration |
| cell_id | string | Cell identifier |
| switch_id | string | Switch identifier |
| roaming_flag | string | Raw roaming flag |
| charge_amount_raw | string | Raw charge amount |
| currency | string | Currency code |
| ingestion_ts | timestamp | Ingestion timestamp |

### Notes

- Values may be invalid or incomplete.
- No business filtering is applied at this stage.
- All records are preserved for audit and replay purposes.

---

## 3. Silver Clean Table

### Table Name

```text
silver_cdr_clean
```

### Purpose

Stores validated, standardized, and deduplicated CDR records.

Only records that pass all quality rules are stored in this table.

### Columns

| Column | Type | Description |
|---|---|---|
| batch_id | string | Batch identifier |
| source_system | string | Source system name |
| source_file | string | Simulated source file name |
| cdr_id | string | CDR identifier |
| subscriber_msisdn | string | Subscriber MSISDN |
| other_party_msisdn | string | Other party MSISDN |
| event_type | string | Standardized event type |
| event_ts | timestamp | Parsed event timestamp |
| event_date | date | Event date |
| event_hour | int | Event hour |
| duration_sec | int | Duration in seconds |
| cell_id | string | Cell identifier |
| switch_id | string | Switch identifier |
| roaming_flag | boolean | Roaming indicator |
| charge_amount | double | Charge amount |
| currency | string | Currency code |
| record_hash | string | Hash value for traceability |
| quality_status | string | Quality status, usually VALID |
| is_duplicate | boolean | Duplicate indicator |
| processed_ts | timestamp | Processing timestamp |

### Notes

- This table is used by Gold aggregate tables.
- Duplicate records are excluded from this table.
- Invalid records are stored in the quarantine table.

---

## 4. Silver Quarantine Table

### Table Name

```text
silver_cdr_quarantine
```

### Purpose

Stores records rejected by data quality checks.

This table allows investigation of invalid records without losing source data.

### Columns

| Column | Type | Description |
|---|---|---|
| batch_id | string | Batch identifier |
| source_system | string | Source system name |
| source_file | string | Simulated source file name |
| cdr_id | string | CDR identifier |
| subscriber_msisdn | string | Subscriber MSISDN |
| event_type | string | Raw event type |
| start_time_raw | string | Raw timestamp |
| duration_raw | string | Raw duration |
| charge_amount_raw | string | Raw charge amount |
| reject_reason | string | Main rejection reason |
| reject_ts | timestamp | Rejection timestamp |

### Example Reject Reasons

```text
INVALID_TIMESTAMP
INVALID_DURATION
UNKNOWN_EVENT_TYPE
MISSING_MSISDN
NEGATIVE_CHARGE_AMOUNT
DUPLICATE_CDR_ID
```

---

## 5. Gold Hourly Usage Table

### Table Name

```text
gold_cdr_usage_hourly
```

### Purpose

Stores hourly usage metrics for dashboard and reporting.

### Grain

One row per:

```text
usage_date + usage_hour + source_system + event_type
```

### Columns

| Column | Type | Description |
|---|---|---|
| usage_date | date | Usage date |
| usage_hour | int | Usage hour |
| event_type | string | Event type |
| source_system | string | Source system |
| total_events | long | Total number of CDR records |
| total_duration_sec | long | Total duration |
| total_charge | double | Total charge amount |
| unique_subscribers | long | Distinct subscriber count |
| roaming_events | long | Number of roaming events |

---

## 6. Gold Subscriber Usage Daily Table

### Table Name

```text
gold_subscriber_usage_daily
```

### Purpose

Stores daily subscriber-level usage metrics.

### Grain

One row per:

```text
usage_date + subscriber_msisdn
```

### Columns

| Column | Type | Description |
|---|---|---|
| usage_date | date | Usage date |
| subscriber_msisdn | string | Subscriber MSISDN |
| voice_call_count | long | Number of VOICE events |
| sms_count | long | Number of SMS events |
| data_session_count | long | Number of DATA events |
| roaming_event_count | long | Number of ROAMING events |
| total_duration_sec | long | Total duration |
| total_charge | double | Total charge amount |

---

## 7. Gold Reconciliation Daily Table

### Table Name

```text
gold_reconciliation_daily
```

### Purpose

Stores batch-level reconciliation results.

### Grain

One row per:

```text
batch_id
```

### Columns

| Column | Type | Description |
|---|---|---|
| batch_id | string | Batch identifier |
| source_system | string | Source system |
| source_file | string | Simulated source file name |
| expected_record_count | int | Expected source record count |
| bronze_record_count | int | Number of Bronze records |
| silver_valid_count | int | Number of Silver Clean records |
| quarantine_count | int | Number of Quarantine records |
| duplicate_count | int | Number of duplicate records |
| reject_rate | double | Quarantine count divided by Bronze count |
| reconciliation_status | string | RECONCILED, WARNING, or FAILED |
| reconciliation_ts | timestamp | Reconciliation timestamp |

## Naming Convention

The project uses simple table names for compatibility with Databricks Free Edition:

```text
metadata_ingestion_log
bronze_cdr_raw
silver_cdr_clean
silver_cdr_quarantine
gold_cdr_usage_hourly
gold_subscriber_usage_daily
gold_reconciliation_daily
```

In a production implementation, schemas or catalogs could be used:

```text
metadata.ingestion_log
bronze.cdr_raw
silver.cdr_clean
silver.cdr_quarantine
gold.cdr_usage_hourly
gold.subscriber_usage_daily
gold.reconciliation_daily
```

## Design Notes

- The Bronze table keeps raw values mostly as strings.
- Silver tables contain typed and validated columns.
- Gold tables are designed for SQL analytics and dashboards.
- Batch metadata is kept separately to support reconciliation.
- Quarantine records are not discarded; they remain available for analysis.
