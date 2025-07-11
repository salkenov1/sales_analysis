{{ config(
    materialized='table',
    description='Fact table for orders'
) }}

SELECT r.order_id AS order_id, d.date AS order_date, r.user_id AS user_id, r.item_id AS item_id, r.payment_id AS payment_id, r.item_quantity AS item_quantity, r.item_price AS item_price, r.item_quantity * r.item_price AS total_price, r.payment_amount AS payment_amount
FROM {{ source('orders', 'raw_data') }} r
LEFT JOIN {{ ref('dim_orders') }} o ON r.order_id = o.order_id
LEFT JOIN {{ ref('dim_users') }} u ON r.user_id = u.user_id
LEFT JOIN {{ ref('dim_items') }} i ON r.item_id = i.item_id
LEFT JOIN {{ ref('dim_payments') }} p ON r.payment_id = p.payment_id
LEFT JOIN {{ ref('dim_dates') }} d ON toDate(o.order_date) = d.date