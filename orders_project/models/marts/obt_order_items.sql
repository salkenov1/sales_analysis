{{ config(
    materialized='table',
    description='Data mart of orders'
) }}

SELECT f.order_id AS order_id, d.date AS order_date, f.user_id AS user_id, u.user_country AS user_country, f.item_id AS item_id,
    i.item_name AS item_name, i.item_category AS item_category, f.item_quantity AS item_quantity, f.item_price AS item_price, f.total_price AS total_price,
    f.payment_id AS payment_id, p.payment_method AS payment_method, p.payment_status AS payment_status, p.payment_currency AS payment_currency,
    f.payment_amount AS payment_amount FROM {{ ref('fct_orders') }} f
LEFT JOIN {{ ref('dim_users') }} u ON f.user_id = u.user_id
LEFT JOIN {{ ref('dim_items') }} i ON f.item_id = i.item_id
LEFT JOIN {{ ref('dim_payments') }} p ON f.payment_id = p.payment_id
LEFT JOIN {{ ref('dim_dates') }} d ON toDate(f.order_date) = d.date