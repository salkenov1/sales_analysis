{{ config(
    materialized='table',
    description='Data mart of category sales'
) }}

SELECT d.date AS order_date, i.item_category AS item_category, COUNT(DISTINCT f.order_id) AS orders_count, SUM(f.item_quantity) AS items_sold, SUM(f.total_price) AS revenue,
AVG(f.item_price) AS avg_price, AVG(f.item_quantity) AS avg_items_per_order FROM {{ ref('fct_orders') }} f
LEFT JOIN {{ ref('dim_items') }} i ON f.item_id = i.item_id
LEFT JOIN {{ ref('dim_dates') }} d ON toDate(f.order_date) = d.date
GROUP BY d.date, i.item_category