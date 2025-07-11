{{ config(
    materialized='table',
    description='Dim table for dates extracted from raw orders'
) }}

WITH date_range AS (
    SELECT toDate('1970-01-01') AS start_date, toDate(MAX(order_date)) AS end_date
    FROM {{ source('orders', 'raw_data') }}
),
dates AS (
    SELECT date_range.start_date + number AS date
    FROM date_range
    ARRAY JOIN range(toUInt32(date_range.end_date - date_range.start_date + 1)) AS number
)

SELECT
    date,
    toYear(date) AS year,
    toMonth(date) AS month,
    toDayOfMonth(date) AS day,
    toDayOfWeek(date) AS weekday
FROM dates