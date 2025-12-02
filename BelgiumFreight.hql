Beeline

CREATE DATABASE IF NOT EXISTS freight_group;
USE freight_group;

-- Raw external table on the single CSV
DROP TABLE IF EXISTS traffic_raw_onecsv;

CREATE EXTERNAL TABLE traffic_raw_onecsv (
  ts_raw            STRING,
  street_id_raw     DOUBLE,
  traffic_count_raw INT,
  traffic_speed_raw DOUBLE
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/freight_onecsv';  -- Bel_60min_0101_0103_2019.csv

-- Validate
SELECT * FROM traffic_raw_onecsv LIMIT 10;


--Sample for Debugging
DROP TABLE IF EXISTS traffic_raw_sample;

CREATE TABLE traffic_raw_sample AS
SELECT *
FROM traffic_raw_onecsv
LIMIT 10000;

SELECT COUNT(*) FROM traffic_raw_sample;
SELECT * FROM traffic_raw_sample LIMIT 5;


--Sample Debugging
DROP TABLE IF EXISTS traffic_sample_cleaned;

CREATE TABLE traffic_sample_cleaned AS
SELECT *
FROM traffic_raw_sample
WHERE CAST(ts_raw AS TIMESTAMP) IS NOT NULL
  AND street_id_raw IS NOT NULL
  AND traffic_count_raw >= 0
  AND traffic_speed_raw >= 0;

SELECT COUNT(*) FROM traffic_sample_cleaned;


--Enriched Sample with Time Buckets
DROP TABLE IF EXISTS traffic_enriched_sample;

CREATE TABLE traffic_enriched_sample
AS
SELECT
  CAST(ts_raw AS TIMESTAMP)                                  AS ts,
  CAST(street_id_raw AS INT)                                 AS street_id,
  traffic_count_raw                                          AS traffic_count,
  traffic_speed_raw                                          AS traffic_speed,
  HOUR(CAST(ts_raw AS TIMESTAMP))                            AS hour_of_day,
  CASE
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 0  AND 2  THEN '12–3 AM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 3  AND 5  THEN '3–6 AM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 6  AND 8  THEN '6–9 AM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 9  AND 11 THEN '9–12 AM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 12 AND 14 THEN '12–3 PM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 15 AND 17 THEN '3–6 PM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 18 AND 20 THEN '6–9 PM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 21 AND 23 THEN '9–12 PM'
  END                                                       AS time_segment,
  CASE
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 0  AND 2  THEN 1
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 3  AND 5  THEN 2
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 6  AND 8  THEN 3
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 9  AND 11 THEN 4
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 12 AND 14 THEN 5
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 15 AND 17 THEN 6
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 18 AND 20 THEN 7
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 21 AND 23 THEN 8
  END                                                       AS time_segment_sort
FROM traffic_sample_cleaned;

-- Debug
SELECT DISTINCT time_segment, time_segment_sort
FROM traffic_enriched_sample
ORDER BY time_segment_sort;


--Full Enriched Dataset with Cleaning
DROP TABLE IF EXISTS traffic_enriched;

CREATE TABLE traffic_enriched AS
SELECT
  CAST(ts_raw AS TIMESTAMP) AS ts,
  CAST(street_id_raw AS INT) AS street_id,
  traffic_count_raw AS traffic_count,
  traffic_speed_raw AS traffic_speed,
  HOUR(CAST(ts_raw AS TIMESTAMP)) AS hour_of_day,
  CASE
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 0  AND 2  THEN '12–3 AM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 3  AND 5  THEN '3–6 AM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 6  AND 8  THEN '6–9 AM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 9  AND 11 THEN '9–12 AM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 12 AND 14 THEN '12–3 PM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 15 AND 17 THEN '3–6 PM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 18 AND 20 THEN '6–9 PM'
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 21 AND 23 THEN '9–12 PM'
  END AS time_segment,
  CASE
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 0  AND 2  THEN 1
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 3  AND 5  THEN 2
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 6  AND 8  THEN 3
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 9  AND 11 THEN 4
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 12 AND 14 THEN 5
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 15 AND 17 THEN 6
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 18 AND 20 THEN 7
    WHEN HOUR(CAST(ts_raw AS TIMESTAMP)) BETWEEN 21 AND 23 THEN 8
  END AS time_segment_sort
FROM traffic_raw_onecsv
WHERE CAST(ts_raw AS TIMESTAMP) IS NOT NULL
  AND street_id_raw IS NOT NULL
  AND traffic_count_raw >= 0
  AND traffic_speed_raw >= 0;


-- Drop any remaining nulls
DROP TABLE IF EXISTS traffic_enriched_clean;

CREATE TABLE traffic_enriched_clean AS
SELECT *
FROM traffic_enriched
WHERE time_segment IS NOT NULL
  AND time_segment_sort IS NOT NULL;

SELECT COUNT(*) AS null_time_segments
FROM traffic_enriched_clean
WHERE time_segment IS NULL
   OR time_segment_sort IS NULL;


--Efficiency Table
DROP TABLE IF EXISTS traffic_efficiency;

CREATE TABLE traffic_efficiency AS
WITH street_stats AS (
  SELECT
    street_id,
    SUM(traffic_count) AS total_traffic_count,
    AVG(traffic_speed) AS avg_traffic_speed
  FROM traffic_enriched_clean
  GROUP BY street_id
)
SELECT
  street_id,
  total_traffic_count,
  avg_traffic_speed,
  (avg_traffic_speed / total_traffic_count) * 1000 AS efficiency_score,
  RANK() OVER (
    ORDER BY (avg_traffic_speed / total_traffic_count) * 1000 DESC
  ) AS efficiency_rank
FROM street_stats;


--Heatmap Export

WITH street_totals AS (
  SELECT
    street_id,
    SUM(traffic_count) AS total_traffic
  FROM traffic_enriched_clean
  GROUP BY street_id
),
top15 AS (
  SELECT street_id
  FROM street_totals
  ORDER BY total_traffic DESC
  LIMIT 15
)
INSERT OVERWRITE DIRECTORY '/user/mbuard/freight_output/heatmap'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
  e.street_id,
  e.time_segment,
  e.time_segment_sort,
  SUM(e.traffic_count) AS sum_traffic_count
FROM traffic_enriched_clean e
JOIN top15 t
  ON e.street_id = t.street_id
GROUP BY
  e.street_id,
  e.time_segment,
  e.time_segment_sort
ORDER BY
  e.street_id,
  e.time_segment_sort;


--Line Chart Export

INSERT OVERWRITE DIRECTORY '/user/mbuard/freight_output/linechart'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
  time_segment,
  time_segment_sort,
  AVG(traffic_speed) AS avg_traffic_speed
FROM traffic_enriched_clean
GROUP BY
  time_segment,
  time_segment_sort
ORDER BY
  time_segment_sort;


--Efficiency Export

INSERT OVERWRITE DIRECTORY '/user/mbuard/freight_output/efficiency'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
  street_id,
  total_traffic_count,
  avg_traffic_speed,
  efficiency_score,
  efficiency_rank
FROM traffic_efficiency
ORDER BY efficiency_rank;
