# Target Architecture

## Purpose

This document describes the target architecture of the Telco CDR Lakehouse Modernization project.

The project simulates how a traditional telecom CDR processing flow can be redesigned using a modern lakehouse architecture.

## Legacy Architecture

A common legacy pattern for CDR processing is:

```text
CDR Source Files
    -> Legacy ETL Tool
        -> Oracle Staging / Core Tables
            -> PL/SQL Packages
                -> Reconciliation and Reporting
```

This pattern is reliable but often has the following challenges:

- Tight coupling between ingestion, transformation, and reporting
- Limited elasticity for large data volumes
- Heavy dependency on relational database processing
- Difficult replay and audit management
- Limited observability across pipeline stages
- Complex operational troubleshooting

## Modern Lakehouse Architecture

The proposed lakehouse pattern is:

```text
CDR Source Systems
    -> Cloud Object Storage Landing Zone
        -> Ingestion Layer
            -> Bronze Raw Layer
                -> Silver Clean + Quarantine Layers
                    -> Gold Analytics + Reconciliation Layers
                        -> SQL Analytics / Dashboard / Reporting
```

## Free Edition Implementation

Because this project is implemented on Databricks Free Edition, the landing zone is simulated.

```text
Generated CDR Batches
    -> Metadata Ingestion Log
        -> Bronze Managed Table
            -> Silver Clean + Quarantine Tables
                -> Gold Aggregate + Reconciliation Tables
                    -> Databricks SQL Queries / Dashboard
```

## Architecture Diagram

```text
[Telco Source Systems]
    |
    |-- MEDIATION_A
    |-- MEDIATION_B
    |-- ROAMING_PARTNER
    |
    v
[Batch / File Simulation]
    |
    v
[Metadata Ingestion Log]
    |
    v
[Bronze Layer: Raw CDR Records]
    |
    +----------------------------+
    |                            |
    v                            v
[Silver Clean Layer]       [Quarantine Layer]
    |
    v
[Gold Layer]
    |-- Hourly Usage Summary
    |-- Subscriber Daily Usage
    |-- Daily Reconciliation
    |
    v
[Databricks SQL Analytics / Dashboard]
```

## Component Responsibilities

| Component | Responsibility |
|---|---|
| Source Systems | Simulate telecom systems producing CDR batches |
| Batch Simulation | Generate file-like CDR data with metadata |
| Metadata Ingestion Log | Track batch, source file, expected count, and status |
| Bronze Layer | Preserve raw CDR records with ingestion metadata |
| Silver Clean Layer | Store validated and standardized records |
| Quarantine Layer | Store rejected records with reject reasons |
| Gold Layer | Produce analytics-ready and reconciliation-ready outputs |
| SQL Analytics | Provide reporting and dashboard-ready queries |

## Production Extension

In a production implementation, the simulated batch layer would be replaced with a real landing zone:

```text
GCS / S3 / ADLS
    -> Databricks Auto Loader
        -> Bronze Delta
```

Additional production components would include:

- Cloud IAM and service principals
- Unity Catalog governance
- External locations
- File notification or incremental file discovery
- Production orchestration
- Monitoring and alerting
- CI/CD
- Data observability

## Design Principle

The main design principle is:

```text
Keep raw data, validate explicitly, quarantine invalid records, reconcile every batch, and serve curated data for analytics.
```
