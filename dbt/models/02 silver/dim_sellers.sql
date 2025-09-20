-- dim_sellers
{{ config(materialized='table') }}

SELECT 
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    CURRENT_TIMESTAMP as created_at
FROM {{ source('raw_data', 'olist_sellers_dataset') }}