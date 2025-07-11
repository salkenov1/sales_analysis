{{ config(
    materialized='table',
    description='Data mart of users daily'
) }}

SELECT f.user_id AS user_id, u.user_country AS user_country, d.date AS order_date, COUNT(DISTINCT f.order_id) AS orders_count, COUNT(f.item_id) AS total_items, COUNT(DISTINCT f.item_id) AS unique_items,
    SUM(f.item_quantity) AS items_purchased, SUM(f.total_price) AS total_spent, AVG(f.total_price) AS avg_order_value
FROM {{ ref('fct_orders') }} f
JOIN {{ ref('dim_users') }} u ON f.user_id = u.user_id
JOIN {{ ref('dim_dates') }} d ON toDate(f.order_date) = d.date
GROUP BY f.user_id, u.user_country, d.date