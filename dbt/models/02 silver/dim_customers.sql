{{ config(materialized='table') }}

SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    CURRENT_TIMESTAMP as created_at
FROM {{ source('raw_data', 'olist_customers_dataset') }}