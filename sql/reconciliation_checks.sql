-- Reconciliation Checks
-- Project: Telco CDR Lakehouse Modernization

-- Reconciliation Status by Batch
SELECT
  batch_id,
  source_system,
  source_file,
  expected_record_count,
  bronze_record_count,
  silver_valid_count,
  quarantine_count,
  duplicate_count,
  reject_rate,
  reconciliation_status
FROM gold_reconciliation_daily
ORDER BY source_system;

-- Reconciliation Status Summary
SELECT
  reconciliation_status,
  COUNT(*) AS batch_count,
  ROUND(AVG(reject_rate), 4) AS avg_reject_rate
FROM gold_reconciliation_daily
GROUP BY reconciliation_status
ORDER BY reconciliation_status;

-- Reject Rate by Source System
SELECT
  source_system,
  SUM(bronze_record_count) AS bronze_record_count,
  SUM(silver_valid_count) AS silver_valid_count,
  SUM(quarantine_count) AS quarantine_count,
  SUM(duplicate_count) AS duplicate_count,
  ROUND(SUM(quarantine_count) / SUM(bronze_record_count), 4) AS reject_rate
FROM gold_reconciliation_daily
GROUP BY source_system
ORDER BY reject_rate DESC;

-- Count Reconciliation Check
SELECT
  batch_id,
  source_system,
  expected_record_count,
  bronze_record_count,
  silver_valid_count,
  quarantine_count,
  silver_valid_count + quarantine_count AS silver_plus_quarantine,
  CASE
    WHEN bronze_record_count = silver_valid_count + quarantine_count THEN 'MATCHED'
    ELSE 'NOT_MATCHED'
  END AS bronze_vs_silver_quarantine_check,
  reconciliation_status
FROM gold_reconciliation_daily
ORDER BY source_system;

-- Failed or Warning Batches
SELECT *
FROM gold_reconciliation_daily
WHERE reconciliation_status IN ('FAILED', 'WARNING')
ORDER BY reconciliation_status, source_system;
