-- Telco CDR Operations Dashboard Queries
-- Project: Telco CDR Lakehouse Modernization

-- 1. Total CDR Records Summary
-- Recommended visualization: Table
SELECT 'Bronze Raw Records' AS metric, COUNT(*) AS value FROM bronze_cdr_raw
UNION ALL
SELECT 'Silver Clean Records' AS metric, COUNT(*) AS value FROM silver_cdr_clean
UNION ALL
SELECT 'Silver Quarantine Records' AS metric, COUNT(*) AS value FROM silver_cdr_quarantine
UNION ALL
SELECT 'Gold Hourly Usage Rows' AS metric, COUNT(*) AS value FROM gold_cdr_usage_hourly
UNION ALL
SELECT 'Gold Subscriber Daily Rows' AS metric, COUNT(*) AS value FROM gold_subscriber_usage_daily;

-- 2. Valid vs Quarantined Records
-- Recommended visualization: Donut / Pie chart
SELECT 'VALID' AS record_status, COUNT(*) AS record_count FROM silver_cdr_clean
UNION ALL
SELECT 'QUARANTINED' AS record_status, COUNT(*) AS record_count FROM silver_cdr_quarantine;

-- 3. Reject Reasons Breakdown
-- Recommended visualization: Horizontal bar chart
SELECT
  reject_reason,
  COUNT(*) AS rejected_record_count
FROM silver_cdr_quarantine
GROUP BY reject_reason
ORDER BY rejected_record_count DESC;

-- 4. Reconciliation Status by Batch
-- Recommended visualization: Table
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

-- 5. Event Type Distribution
-- Recommended visualization: Bar chart
SELECT
  event_type,
  SUM(total_events) AS total_events,
  ROUND(SUM(total_charge), 2) AS total_charge,
  SUM(total_duration_sec) AS total_duration_sec
FROM gold_cdr_usage_hourly
GROUP BY event_type
ORDER BY total_events DESC;

-- 6. Hourly Usage Trend
-- Recommended visualization: Line chart
SELECT
  usage_hour,
  SUM(total_events) AS total_events,
  SUM(total_duration_sec) AS total_duration_sec,
  ROUND(SUM(total_charge), 2) AS total_charge
FROM gold_cdr_usage_hourly
GROUP BY usage_hour
ORDER BY usage_hour;

-- 7. Top 10 Subscribers by Charge
-- Recommended visualization: Table
SELECT
  subscriber_msisdn,
  SUM(voice_call_count) AS voice_call_count,
  SUM(sms_count) AS sms_count,
  SUM(data_session_count) AS data_session_count,
  SUM(roaming_event_count) AS roaming_event_count,
  SUM(total_duration_sec) AS total_duration_sec,
  ROUND(SUM(total_charge), 2) AS total_charge
FROM gold_subscriber_usage_daily
GROUP BY subscriber_msisdn
ORDER BY total_charge DESC
LIMIT 10;

-- 8. Source System Usage Summary
-- Recommended visualization: Bar chart / Table
-- Note: unique_subscribers_sum is summed from hourly distinct counts.
SELECT
  source_system,
  SUM(total_events) AS total_events,
  SUM(total_duration_sec) AS total_duration_sec,
  ROUND(SUM(total_charge), 2) AS total_charge,
  SUM(unique_subscribers) AS unique_subscribers_sum,
  SUM(roaming_events) AS roaming_events
FROM gold_cdr_usage_hourly
GROUP BY source_system
ORDER BY total_events DESC;

-- 9. Reconciliation Status Summary
-- Recommended visualization: Bar chart / Pie chart
SELECT
  reconciliation_status,
  COUNT(*) AS batch_count,
  ROUND(AVG(reject_rate), 4) AS avg_reject_rate
FROM gold_reconciliation_daily
GROUP BY reconciliation_status
ORDER BY reconciliation_status;

-- 10. Reject Rate by Source System
-- Recommended visualization: Bar chart
SELECT
  source_system,
  SUM(bronze_record_count) AS bronze_record_count,
  SUM(silver_valid_count) AS silver_valid_count,
  SUM(quarantine_count) AS quarantine_count,
  ROUND(SUM(quarantine_count) / SUM(bronze_record_count), 4) AS reject_rate
FROM gold_reconciliation_daily
GROUP BY source_system
ORDER BY reject_rate DESC;

-- 11. Daily Usage Summary
-- Recommended visualization: Table / Line chart
SELECT
  usage_date,
  SUM(total_events) AS total_events,
  SUM(total_duration_sec) AS total_duration_sec,
  ROUND(SUM(total_charge), 2) AS total_charge,
  SUM(roaming_events) AS roaming_events
FROM gold_cdr_usage_hourly
GROUP BY usage_date
ORDER BY usage_date;

-- 12. Quality Summary by Source System
-- Recommended visualization: Table / Stacked bar chart
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
