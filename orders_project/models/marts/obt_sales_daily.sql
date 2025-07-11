{{ config(
    materialized='table',
    description='Data mart of daily sales'
) }}

SELECT
    d.date AS order_date, COUNT(DISTINCT f.order_id) AS total_orders, COUNT(DISTINCT f.user_id) AS unique_users, SUM(f.item_quantity) AS items_sold, SUM(f.total_price) AS total_revenue,
    AVG(f.total_price) AS avg_order_value, AVG(f.item_quantity) AS avg_items_per_order
FROM {{ ref('fct_orders') }} f
LEFT JOIN {{ ref('dim_dates') }} d ON toDate(f.order_date) = d.date
GROUP BY d.date