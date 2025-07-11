{{ config(
    materialized='table',
    description='Data mart of payments'
) }}

SELECT d.date AS payment_date, p.payment_method, p.payment_status, COUNT(DISTINCT f.payment_id) AS total_payments, COUNT(DISTINCT f.order_id) AS total_orders, 
    SUM(f.payment_amount) AS total_paid, AVG(f.payment_amount) AS avg_payment
FROM {{ ref('fct_orders') }} f
LEFT JOIN {{ ref('dim_payments') }} p ON f.payment_id = p.payment_id
LEFT JOIN {{ ref('dim_dates') }} d ON toDate(f.order_date) = d.date
GROUP BY d.date, p.payment_method, p.payment_status