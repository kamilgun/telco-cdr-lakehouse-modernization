-- Data Quality Summary Queries
-- Project: Telco CDR Lakehouse Modernization

-- Valid vs Quarantined Records
SELECT 'VALID' AS record_status, COUNT(*) AS record_count FROM silver_cdr_clean
UNION ALL
SELECT 'QUARANTINED' AS record_status, COUNT(*) AS record_count FROM silver_cdr_quarantine;

-- Reject Reasons Breakdown
SELECT
  reject_reason,
  COUNT(*) AS rejected_record_count
FROM silver_cdr_quarantine
GROUP BY reject_reason
ORDER BY rejected_record_count DESC;

-- Quality Summary by Source System
WITH clean_counts AS (
  SELECT source_system, COUNT(*) AS clean_count
  FROM silver_cdr_clean
  GROUP BY source_system
),
quarantine_counts AS (
  SELECT source_system, COUNT(*) AS quarantine_count
  FROM silver_cdr_quarantine
  GROUP BY source_system
)
SELECT
  COALESCE(c.source_system, q.source_system) AS source_system,
  COALESCE(c.clean_count, 0) AS clean_count,
  COALESCE(q.quarantine_count, 0) AS quarantine_count,
  COALESCE(c.clean_count, 0) + COALESCE(q.quarantine_count, 0) AS total_count,
  ROUND(
    COALESCE(q.quarantine_count, 0) /
    (COALESCE(c.clean_count, 0) + COALESCE(q.quarantine_count, 0)),
    4
  ) AS quarantine_rate
FROM clean_counts c
FULL OUTER JOIN quarantine_counts q
  ON c.source_system = q.source_system
ORDER BY quarantine_rate DESC;

-- Quarantine Detail Sample
SELECT
  batch_id,
  source_system,
  source_file,
  cdr_id,
  subscriber_msisdn,
  event_type,
  start_time_raw,
  duration_raw,
  charge_amount_raw,
  reject_reason,
  reject_ts
FROM silver_cdr_quarantine
ORDER BY reject_ts DESC
LIMIT 100;
