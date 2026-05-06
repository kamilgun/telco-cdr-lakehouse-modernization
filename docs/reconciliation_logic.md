# Reconciliation Logic

## Purpose

This document describes the reconciliation logic used in the Telco CDR Lakehouse Modernization project.

In telecom data platforms, reconciliation is critical because CDR records are often used for reporting, billing support, revenue assurance, and operational monitoring.

The goal of reconciliation is to ensure that records received from source systems are correctly processed, validated, rejected when necessary, and made available for downstream analytics.

## Reconciliation Scope

This project implements batch-level reconciliation.

Each simulated source file or batch is tracked with a unique batch identifier.

The reconciliation process compares:

- Expected record count
- Bronze raw record count
- Silver valid record count
- Quarantine record count
- Duplicate record count

## Batch Concept

In this project, a batch simulates a source CDR file.

Example batch:

```text
batch_id: BATCH_20260421_101500_MEDIATION_A
source_system: MEDIATION_A
source_file: cdr_voice_20260421_101500.csv
expected_record_count: 1000
arrival_ts: 2026-04-21 10:15:00


In Databricks Free Edition, the physical source file is simulated through generated records and metadata.

In a production implementation, the batch would typically represent a real file arriving in a cloud object storage landing zone such as GCS, S3, or ADLS.

Reconciliation Tables

The reconciliation process uses the following tables:

Metadata Table
metadata_ingestion_log

This table stores expected batch information.

Example columns:

Column	Description
batch_id	Unique batch identifier
source_system	Source system name
source_file	Simulated source file name
arrival_ts	Batch arrival timestamp
expected_record_count	Expected number of records
actual_generated_count	Number of generated records
bronze_count	Number of records written to Bronze
status	Batch status
Bronze Table
bronze_cdr_raw

This table stores raw ingested CDR records.

Silver Clean Table
silver_cdr_clean

This table stores valid and standardized CDR records.

Quarantine Table
silver_cdr_quarantine

This table stores invalid or rejected records.

Gold Reconciliation Table
gold_reconciliation_daily

This table stores the final reconciliation result for each batch.

Core Reconciliation Formula

The main reconciliation formula is:

bronze_record_count = silver_valid_count + quarantine_count

This means every raw record must end up in one of two places:

Silver Clean
or
Silver Quarantine

No record should disappear silently.

Expected Count Validation

The second reconciliation formula compares the expected source count with the Bronze count:

expected_record_count = bronze_record_count

If this check fails, it may indicate an ingestion issue.

Reconciliation Status

The project uses three reconciliation statuses:

Status	Meaning
RECONCILED	Counts match successfully
WARNING	Records were processed but quality/reject rate needs attention
FAILED	Source, Bronze, Silver, or Quarantine counts do not match
Status Logic
RECONCILED

A batch is marked as RECONCILED when:

expected_record_count = bronze_record_count
AND bronze_record_count = silver_valid_count + quarantine_count
WARNING

A batch is marked as WARNING when the count reconciliation is technically correct but the reject rate is higher than the defined threshold.

Example threshold:

reject_rate > 10%
FAILED

A batch is marked as FAILED when:

expected_record_count <> bronze_record_count
OR bronze_record_count <> silver_valid_count + quarantine_count
Reconciliation Output Table

Final table:

gold_reconciliation_daily

Expected columns:

Column	Description
batch_id	Unique batch identifier
source_system	Source system
source_file	Source file name
expected_record_count	Expected number of records
bronze_record_count	Raw records in Bronze
silver_valid_count	Valid records in Silver Clean
quarantine_count	Invalid records in Quarantine
duplicate_count	Duplicate records detected
reject_rate	Quarantine count / Bronze count
reconciliation_status	RECONCILED, WARNING, or FAILED
reconciliation_ts	Reconciliation timestamp
Example Output
batch_id: BATCH_20260421_101500_MEDIATION_A
source_system: MEDIATION_A
source_file: cdr_voice_20260421_101500.csv
expected_record_count: 1000
bronze_record_count: 1000
silver_valid_count: 930
quarantine_count: 70
duplicate_count: 8
reject_rate: 7.0%
reconciliation_status: RECONCILED

Another example:

batch_id: BATCH_20260421_103000_MEDIATION_B
source_system: MEDIATION_B
source_file: cdr_data_20260421_103000.csv
expected_record_count: 1000
bronze_record_count: 1000
silver_valid_count: 850
quarantine_count: 150
duplicate_count: 20
reject_rate: 15.0%
reconciliation_status: WARNING

Failed example:

batch_id: BATCH_20260421_104500_ROAMING_PARTNER
source_system: ROAMING_PARTNER
source_file: cdr_roaming_20260421_104500.csv
expected_record_count: 1000
bronze_record_count: 980
silver_valid_count: 920
quarantine_count: 60
duplicate_count: 5
reject_rate: 6.1%
reconciliation_status: FAILED
Dashboard Metrics

The reconciliation output can be used to build the following dashboard metrics:

Reconciliation status by batch
Expected vs Bronze record count
Valid vs Quarantine count
Reject rate by source system
Duplicate count by source system
Failed batches
Warning batches
Daily reconciliation trend
Operational Usage

In a production environment, reconciliation results could be used for:

Alerting operations teams
Triggering reprocessing workflows
Blocking downstream reporting
Supporting revenue assurance controls
Auditing source file completeness
Monitoring data quality trends
Production Enhancements

Potential production enhancements include:

File checksum validation
Source control total validation
Control file comparison
SLA-based late file detection
Automated reprocessing
Alerting on failed reconciliation
Integration with orchestration tools
Historical reconciliation trend analysis

---

# 3. Sonra küçük bir güncelleme: README içindeki doküman linkleri

README içinde `Key Features` bölümünden sonra veya `Repository Structure` bölümünden önce şu bölümü ekle:

```markdown
## Documentation

Detailed project documentation is available under the `docs/` folder:

- [Project Scope](docs/project_scope.md)
- [Data Quality Rules](docs/data_quality_rules.md)
- [Reconciliation Logic](docs/reconciliation_logic.md)
- [Free Edition Notes](docs/free_edition_notes.md)