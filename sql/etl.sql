/* =========================================================
   London Bike Riders ETL (SSMS / SQL Server)
   File: /sql/etl.sql
   ========================================================= */

------------------------------------------------------------
-- 1) STAGING: types + trimming
------------------------------------------------------------
IF OBJECT_ID('dbo.stg_bike', 'U') IS NOT NULL DROP TABLE dbo.stg_bike;

SELECT
    TRY_CAST(id AS BIGINT) AS row_id,                    -- if your data has no id, we'll generate later
    TRY_CAST([date] AS DATETIME) AS dt,                  -- change [date] to your timestamp column
    TRY_CAST(cnt AS INT) AS ride_count,                  -- change cnt to your count column

    TRY_CAST(temp AS FLOAT) AS temp_c,                   -- adjust if temp is already in C
    TRY_CAST(hum AS FLOAT) AS humidity,                  -- often 0..1 or 0..100
    TRY_CAST(windspeed AS FLOAT) AS wind_speed,          -- adjust unit label if needed

    NULLIF(LTRIM(RTRIM(CAST(season AS NVARCHAR(50)))), '') AS season,
    NULLIF(LTRIM(RTRIM(CAST(weather AS NVARCHAR(50)))), '') AS weather,   -- if code, keep as string
    TRY_CAST(is_holiday AS BIT) AS is_holiday,
    TRY_CAST(is_weekend AS BIT) AS is_weekend
INTO dbo.stg_bike
FROM dbo.raw_bike;

------------------------------------------------------------
-- 2) CLEAN: remove bad rows, engineer time features
------------------------------------------------------------
IF OBJECT_ID('dbo.clean_bike', 'U') IS NOT NULL DROP TABLE dbo.clean_bike;

WITH base AS (
    SELECT
        COALESCE(row_id, ROW_NUMBER() OVER (ORDER BY (SELECT 1))) AS row_id,
        dt,
        ride_count,
        temp_c,
        humidity,
        wind_speed,
        COALESCE(season, 'Unknown') AS season,
        COALESCE(weather, 'Unknown') AS weather,
        COALESCE(is_holiday, 0) AS is_holiday,
        COALESCE(is_weekend, 0) AS is_weekend
    FROM dbo.stg_bike
)
SELECT
    row_id,
    dt,
    ride_count,
    temp_c,
    CASE
      WHEN humidity IS NULL THEN NULL
      WHEN humidity BETWEEN 0 AND 1 THEN humidity * 100
      ELSE humidity
    END AS humidity_pct,
    wind_speed,
    season,
    weather,
    is_holiday,
    is_weekend,

    -- time features
    CAST(dt AS DATE) AS date_key,
    YEAR(dt) AS [year],
    MONTH(dt) AS [month],
    DATENAME(MONTH, dt) AS month_name,
    DATEPART(QUARTER, dt) AS [quarter],
    DATENAME(WEEKDAY, dt) AS weekday_name,
    DATEPART(WEEKDAY, dt) AS weekday_num,
    DATEPART(HOUR, dt) AS [hour],

    -- weekday vs weekend label
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type
INTO dbo.clean_bike
FROM base
WHERE dt IS NOT NULL
  AND ride_count IS NOT NULL
  AND ride_count >= 0;

------------------------------------------------------------
-- 3) STAR SCHEMA (recommended for Power BI)
------------------------------------------------------------

-- DimDate
IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL DROP TABLE dbo.dim_date;

SELECT DISTINCT
    date_key,
    [year],
    [month],
    month_name,
    [quarter],
    weekday_name,
    weekday_num,
    day_type
INTO dbo.dim_date
FROM dbo.clean_bike;

ALTER TABLE dbo.dim_date
ADD CONSTRAINT PK_dim_date PRIMARY KEY (date_key);

-- DimWeather (keeps season + weather)
IF OBJECT_ID('dbo.dim_weather', 'U') IS NOT NULL DROP TABLE dbo.dim_weather;

SELECT
    ROW_NUMBER() OVER (ORDER BY season, weather) AS weather_key,
    season,
    weather
INTO dbo.dim_weather
FROM (
    SELECT DISTINCT season, weather
    FROM dbo.clean_bike
) w;

ALTER TABLE dbo.dim_weather
ADD CONSTRAINT PK_dim_weather PRIMARY KEY (weather_key);

-- FactRides
IF OBJECT_ID('dbo.fact_rides', 'U') IS NOT NULL DROP TABLE dbo.fact_rides;

SELECT
    c.row_id,
    c.date_key,
    w.weather_key,
    c.[hour],
    c.is_holiday,
    c.ride_count,
    c.temp_c,
    c.humidity_pct,
    c.wind_speed
INTO dbo.fact_rides
FROM dbo.clean_bike c
JOIN dbo.dim_weather w
  ON w.season = c.season
 AND w.weather = c.weather;

ALTER TABLE dbo.fact_rides
ADD CONSTRAINT PK_fact_rides PRIMARY KEY (row_id);

------------------------------------------------------------
-- 4) Power BI-friendly view (denormalized)
------------------------------------------------------------
IF OBJECT_ID('dbo.vw_bike_model', 'V') IS NOT NULL DROP VIEW dbo.vw_bike_model;
GO
CREATE VIEW dbo.vw_bike_model AS
SELECT
    f.row_id,
    d.date_key,
    d.[year],
    d.[month],
    d.month_name,
    d.[quarter],
    d.weekday_name,
    d.day_type,
    f.[hour],
    f.is_holiday,
    w.season,
    w.weather,
    f.ride_count,
    f.temp_c,
    f.humidity_pct,
    f.wind_speed
FROM dbo.fact_rides f
JOIN dbo.dim_date d     ON d.date_key = f.date_key
JOIN dbo.dim_weather w  ON w.weather_key = f.weather_key;
GO
