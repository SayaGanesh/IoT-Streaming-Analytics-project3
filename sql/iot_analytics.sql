-- ============================================================
-- IoT Streaming Analytics – Historical SQL Queries
-- Target: Azure Synapse Analytics (Serverless SQL Pool)
-- Source: Silver Delta tables in ADLS Gen2
-- ============================================================

-- ------------------------------------------------------------
-- 1. Device Anomaly Summary – Last 7 Days
-- ------------------------------------------------------------
SELECT
    device_id,
    device_type,
    factory_zone,
    COUNT(*) FILTER (WHERE is_anomaly = true)    AS anomaly_windows,
    COUNT(*)                                      AS total_windows,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_anomaly = true) / COUNT(*), 2) AS anomaly_rate_pct,
    ROUND(AVG(avg_temp_c), 2)                    AS avg_temp_7d,
    ROUND(MAX(max_temp_c), 2)                    AS peak_temp_7d,
    ROUND(AVG(avg_vibration), 3)                 AS avg_vibration_7d
FROM
    OPENROWSET(
        BULK 'silver/iot_windows/',
        DATA_SOURCE = 'silver_iot_storage',
        FORMAT = 'DELTA'
    ) AS r
WHERE window_start >= DATEADD(DAY, -7, GETDATE())
GROUP BY device_id, device_type, factory_zone
ORDER BY anomaly_rate_pct DESC;

-- ------------------------------------------------------------
-- 2. Hourly Anomaly Heatmap (for dashboards)
-- ------------------------------------------------------------
SELECT
    CAST(window_start AS DATE)          AS event_date,
    DATEPART(HOUR, window_start)        AS hour_of_day,
    factory_zone,
    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    COUNT(*)                            AS total_windows
FROM
    OPENROWSET(
        BULK 'silver/iot_windows/',
        DATA_SOURCE = 'silver_iot_storage',
        FORMAT = 'DELTA'
    ) AS r
GROUP BY CAST(window_start AS DATE), DATEPART(HOUR, window_start), factory_zone
ORDER BY event_date, hour_of_day, factory_zone;

-- ------------------------------------------------------------
-- 3. Device Temperature Trend – Rolling 1-Hour Average
-- ------------------------------------------------------------
SELECT
    device_id,
    window_start,
    avg_temp_c,
    AVG(avg_temp_c) OVER (
        PARTITION BY device_id
        ORDER BY window_start
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_1hr_avg_temp,
    MAX(avg_temp_c) OVER (
        PARTITION BY device_id
        ORDER BY window_start
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_1hr_max_temp
FROM
    OPENROWSET(
        BULK 'silver/iot_windows/',
        DATA_SOURCE = 'silver_iot_storage',
        FORMAT = 'DELTA'
    ) AS r
WHERE device_id = 'DEVICE-001'
ORDER BY window_start;

-- ------------------------------------------------------------
-- 4. CRITICAL vs WARNING Anomaly Breakdown by Zone
-- ------------------------------------------------------------
SELECT
    factory_zone,
    anomaly_severity,
    COUNT(*)                              AS event_count,
    ROUND(AVG(avg_temp_c), 2)            AS avg_temp_at_anomaly,
    ROUND(AVG(avg_vibration), 3)         AS avg_vibration_at_anomaly
FROM
    OPENROWSET(
        BULK 'silver/iot_windows/',
        DATA_SOURCE = 'silver_iot_storage',
        FORMAT = 'DELTA'
    ) AS r
WHERE is_anomaly = 1
GROUP BY factory_zone, anomaly_severity
ORDER BY factory_zone, anomaly_severity;

-- ------------------------------------------------------------
-- 5. Mean Time Between Failures (MTBF proxy) per Device
-- ------------------------------------------------------------
WITH anomaly_events AS (
    SELECT
        device_id,
        window_start,
        LAG(window_start) OVER (PARTITION BY device_id ORDER BY window_start) AS prev_anomaly
    FROM
        OPENROWSET(
            BULK 'silver/iot_windows/',
            DATA_SOURCE = 'silver_iot_storage',
            FORMAT = 'DELTA'
        ) AS r
    WHERE is_anomaly = 1
)
SELECT
    device_id,
    COUNT(*) AS total_anomalies,
    ROUND(AVG(DATEDIFF(MINUTE, prev_anomaly, window_start)) / 60.0, 2) AS avg_hours_between_anomalies
FROM anomaly_events
WHERE prev_anomaly IS NOT NULL
GROUP BY device_id
ORDER BY avg_hours_between_anomalies ASC;
