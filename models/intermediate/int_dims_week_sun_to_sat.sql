{{ config(materialized='table') }}

/* 

This table generates the weekly boundaries for a date spine that it is suitable for Financial Reporting, especially in this case it use to 
break down Monthly Budget into weekly Budget 

Each week starts on Sunday and ends on Saturday   
*/
WITH date_spine AS (

    -- Generate one row per day from some min to max date
    SELECT
        date_day::DATE AS date_day
    FROM generate_series(
        '2021-01-01'::DATE,
        current_date + interval '1 year', 
        interval '1 day'
    ) AS t(date_day)

),

week_boundaries AS (

    SELECT
        date_day,
        -- Start of week = previous (or same) Sunday
        date_day - EXTRACT(DOW FROM date_day)::INT AS week_start_date,
        -- End of week = following Saturday
        date_day - EXTRACT(DOW FROM date_day)::INT + 6 AS week_end_date
    FROM date_spine
)

, month_spine AS (
    SELECT 
        (DATE_TRUNC('month', d))::DATE AS month_start
    FROM GENERATE_SERIES(
        '2024-01-01'::DATE,
        '2026-12-01'::DATE,
        INTERVAL '1 month'
    ) AS gs(d)
)

, month_year AS (
    SELECT
        EXTRACT(YEAR  FROM month_start)::INT  AS year,
        EXTRACT(MONTH FROM month_start)::INT  AS month,
        EXTRACT(DAY   FROM (month_start + INTERVAL '1 month - 1 day'))::INT AS days_in_month
    FROM month_spine
    ORDER BY year, month
)

, week_boundary AS (
    SELECT 
        DISTINCT
        week_start_date,
        week_end_date,
        EXTRACT(MONTH FROM week_start_date)::INT AS month_start_num,  -- e.g., 1 for Jan
        EXTRACT(YEAR  FROM week_start_date)::INT AS year_start_num,   -- e.g., 2024
        EXTRACT(MONTH FROM week_end_date)::INT   AS month_end_num,     -- e.g., 2 for Feb
        EXTRACT(YEAR  FROM week_end_date)::INT   AS year_end_num,       -- e.g., 2024
        /* Days of the week that lie in the start month */
        GREATEST(
            0,
            LEAST(
                week_end_date,
                (DATE_TRUNC('month', week_start_date) + INTERVAL '1 month - 1 day')::DATE
            ) - week_start_date + 1
        )::INT AS days_in_month_start_in_week,

        /* Days of the week that lie in the end month (0 if same month) */
        CASE
            WHEN DATE_TRUNC('month', week_start_date) = DATE_TRUNC('month', week_end_date)
                THEN 0
            ELSE (7 - GREATEST(
                        0,
                        LEAST(
                            week_end_date,
                            (DATE_TRUNC('month', week_start_date) + INTERVAL '1 month - 1 day')::DATE
                        ) - week_start_date + 1
                    ))
        END::INT AS days_in_month_end_in_week

    FROM week_boundaries
    ORDER BY week_start_date
)

SELECT * FROM week_boundary