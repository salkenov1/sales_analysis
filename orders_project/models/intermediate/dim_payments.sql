{{ config(
    materialized='table',
    description='Dim table for payments extracted from raw orders'
) }}

SELECT DISTINCT payment_id, payment_method, payment_status, payment_currency
FROM {{ source('orders', 'raw_data') }}