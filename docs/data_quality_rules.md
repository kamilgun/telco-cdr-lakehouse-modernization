# Data Quality Rules

## Purpose

This document defines the data quality rules used in the Telco CDR Lakehouse Modernization project.

In telecom CDR processing, data quality is critical because CDR records may be used for reporting, reconciliation, billing support, revenue assurance, customer analytics, and operational monitoring.

Invalid or duplicate records must not be silently ignored. They should be isolated, tracked, and made available for investigation.

## Data Quality Strategy

The project follows a simple but production-aware quality strategy:

```text
Bronze Raw Records
    -> Apply quality rules
        -> Valid records go to Silver Clean
        -> Invalid records go to Silver Quarantine
		
		
The Bronze layer preserves all raw records.

The Silver Clean layer contains only validated and standardized records.

The Quarantine layer contains invalid or rejected records together with a clear reject reason.

Quality Rule Summary
Rule ID	Rule Name	Description	Reject Reason
DQ_001	Timestamp validation	Event timestamp must be parseable	INVALID_TIMESTAMP
DQ_002	Duration validation	Duration must be positive	INVALID_DURATION
DQ_003	Event type validation	Event type must be one of the accepted values	UNKNOWN_EVENT_TYPE
DQ_004	Subscriber MSISDN validation	Subscriber MSISDN must not be null or empty	MISSING_MSISDN
DQ_005	Charge amount validation	Charge amount must not be negative	NEGATIVE_CHARGE_AMOUNT
DQ_006	Duplicate detection	CDR ID must be unique per source system	DUPLICATE_CDR_ID
Accepted Event Types

The project accepts the following event types:

VOICE
SMS
DATA
ROAMING

Any other event type is rejected with:

UNKNOWN_EVENT_TYPE
Rule Details
DQ_001 - Timestamp Validation
Description

The raw event timestamp field must be parseable into a valid timestamp.

Source Column
start_time_raw
Target Column
event_ts
Validation Logic
event_ts IS NOT NULL
Reject Reason
INVALID_TIMESTAMP
Example Invalid Values
2026-99-99 25:61:00
not_a_timestamp
NULL
DQ_002 - Duration Validation
Description

The event duration must be a positive numeric value.

For SMS events, duration can be represented as a small positive value in this project simulation.

Source Column
duration_raw
Target Column
duration_sec
Validation Logic
duration_sec > 0
Reject Reason
INVALID_DURATION
Example Invalid Values
0
-10
NULL
abc
DQ_003 - Event Type Validation
Description

The event type must belong to the accepted set of CDR event types.

Source Column
event_type
Validation Logic
event_type IN ('VOICE', 'SMS', 'DATA', 'ROAMING')
Reject Reason
UNKNOWN_EVENT_TYPE
Example Invalid Values
UNKNOWN
MMS
INVALID_EVENT
NULL
DQ_004 - Subscriber MSISDN Validation
Description

The subscriber MSISDN identifies the customer or subscriber that generated the event.

This field must not be null or empty.

Source Column
subscriber_msisdn
Validation Logic
subscriber_msisdn IS NOT NULL
AND subscriber_msisdn <> ''
Reject Reason
MISSING_MSISDN
DQ_005 - Charge Amount Validation
Description

The charge amount must not be negative.

Zero charge is allowed because some events may be included in bundles, free allowances, or promotional packages.

Source Column
charge_amount_raw
Target Column
charge_amount
Validation Logic
charge_amount >= 0
Reject Reason
NEGATIVE_CHARGE_AMOUNT
Example Invalid Values
-0.50
-10.00
DQ_006 - Duplicate Detection
Description

A CDR record must be unique within a source system.

The project uses the following business key for duplicate detection:

source_system + cdr_id

If more than one record exists for the same business key, only the first record is kept in the Silver Clean layer. Duplicate records are moved to the Quarantine layer.

Validation Logic
ROW_NUMBER() OVER (
    PARTITION BY source_system, cdr_id
    ORDER BY ingestion_ts
) = 1
Reject Reason
DUPLICATE_CDR_ID
Reject Reason Priority

A record may violate more than one rule. In this project, the reject reason priority is defined as follows:

MISSING_MSISDN
INVALID_TIMESTAMP
INVALID_DURATION
NEGATIVE_CHARGE_AMOUNT
UNKNOWN_EVENT_TYPE
DUPLICATE_CDR_ID

This means if a record has both a missing MSISDN and invalid duration, the reject reason will be:

MISSING_MSISDN

This simplifies reporting and makes the quarantine summary easier to interpret.

Quarantine Table

Invalid records are stored in:

silver_cdr_quarantine

Expected columns:

Column	Description
batch_id	Simulated batch identifier
source_system	Source system name
source_file	Simulated source file name
cdr_id	CDR identifier
subscriber_msisdn	Subscriber MSISDN
event_type	Raw event type
start_time_raw	Raw timestamp
duration_raw	Raw duration
charge_amount_raw	Raw charge amount
reject_reason	Main rejection reason
reject_ts	Rejection timestamp
Quality Metrics

The following quality metrics will be calculated for monitoring and dashboarding:

Metric	Description
total_bronze_records	Number of raw records
valid_records	Number of records loaded into Silver Clean
quarantined_records	Number of rejected records
duplicate_records	Number of duplicate records
reject_rate	Quarantined records / total bronze records
reject_reason_count	Number of records by reject reason
Example Quality Summary
Total Bronze Records: 3000
Valid Records: 2715
Quarantined Records: 285
Reject Rate: 9.5%

Reject Reasons:
- INVALID_DURATION: 140
- UNKNOWN_EVENT_TYPE: 55
- MISSING_MSISDN: 40
- INVALID_TIMESTAMP: 25
- NEGATIVE_CHARGE_AMOUNT: 15
- DUPLICATE_CDR_ID: 10
Production Considerations

In a production environment, data quality rules could be extended with:

Reference data validation
MSISDN format validation
Cell ID validation
Roaming partner validation
Event sequence validation
Threshold-based anomaly detection
Alerting on reject rate increase
Data quality scorecards
Integration with data observability tools		