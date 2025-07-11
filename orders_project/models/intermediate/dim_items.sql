{{ config(
    materialized='table',
    description='Dim table for items extracted from raw orders'
) }}

SELECT DISTINCT item_id, item_name, item_category
FROM {{ source('orders', 'raw_data') }}