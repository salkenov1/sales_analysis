{{ config(
    materialized='table',
    description='Dim table for users extracted from raw orders'
) }}

SELECT distinct user_id, user_name, user_email, user_country
FROM {{ source('orders', 'raw_data')}}