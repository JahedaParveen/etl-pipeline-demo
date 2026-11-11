-- Reporting views. Applied automatically by the pipeline on every run.

-- Daily summary per city, using each city's local date (from the cities CSV).
CREATE OR REPLACE VIEW daily_city_weather AS
SELECT
    c.name                                         AS city,
    c.country,
    (w.observed_at AT TIME ZONE c.timezone)::date  AS local_date,
    COUNT(*)                                       AS hours,
    ROUND(AVG(w.temperature_c), 1)                 AS avg_temp_c,
    MIN(w.temperature_c)                           AS min_temp_c,
    MAX(w.temperature_c)                           AS max_temp_c,
    ROUND(SUM(w.precipitation_mm), 1)              AS total_precip_mm,
    MAX(w.wind_speed_kmh)                          AS max_wind_kmh
FROM weather_hourly w
JOIN cities c ON c.id = w.city_id
GROUP BY c.name, c.country, (w.observed_at AT TIME ZONE c.timezone)::date;

-- The most recent reading for each city.
CREATE OR REPLACE VIEW latest_city_weather AS
SELECT DISTINCT ON (c.id)
    c.name                                   AS city,
    c.country,
    w.observed_at,
    (w.observed_at AT TIME ZONE c.timezone)  AS local_time,
    w.temperature_c,
    w.relative_humidity_pct,
    w.precipitation_mm,
    w.wind_speed_kmh
FROM weather_hourly w
JOIN cities c ON c.id = w.city_id
ORDER BY c.id, w.observed_at DESC;
