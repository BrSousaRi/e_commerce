{{ config(materialized='table') }}

SELECT 
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.shipping_limit_date::timestamp as shipping_limit_date,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value as total_item_value,
    CURRENT_TIMESTAMP as created_at
FROM {{ source('raw_data', 'olist_order_items_dataset') }} oi