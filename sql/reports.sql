-- Example report queries. Run them all with:
--   docker compose exec postgres psql -U etl -d weather -f /sql/reports.sql
-- or paste them into any Postgres client (host localhost, port 5433).

-- 1. Average temperature per city per day (local date)
SELECT city, local_date, hours, avg_temp_c, min_temp_c, max_temp_c, total_precip_mm
FROM daily_city_weather
ORDER BY local_date DESC, city;

-- 2. Latest reading for every city
SELECT city, country, local_time, temperature_c, relative_humidity_pct, wind_speed_kmh
FROM latest_city_weather
ORDER BY temperature_c DESC;

-- 3. Hottest and wettest cities over the last 24 hours
SELECT c.name AS city,
       ROUND(AVG(w.temperature_c), 1)    AS avg_temp_c,
       ROUND(SUM(w.precipitation_mm), 1) AS precip_mm
FROM weather_hourly w
JOIN cities c ON c.id = w.city_id
WHERE w.observed_at > now() - interval '24 hours'
GROUP BY c.name
ORDER BY avg_temp_c DESC;

-- 4. Pipeline run history
SELECT id, status, started_at, finished_at - started_at AS duration,
       rows_extracted, rows_inserted, rows_updated, rows_unchanged, rows_rejected,
       error_message
FROM pipeline_runs
ORDER BY id DESC
LIMIT 20;

-- 5. Why rows were rejected
SELECT reason, COUNT(*) AS rows, MAX(rejected_at) AS last_seen
FROM rejected_rows
GROUP BY reason
ORDER BY rows DESC;
