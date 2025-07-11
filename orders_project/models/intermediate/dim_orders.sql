{{ config(
    materialized='table',
    description='Dim table for orders extracted from raw orders'
) }}

SELECT DISTINCT order_id, user_id, order_date, payment_id
FROM {{ source('orders', 'raw_data') }}