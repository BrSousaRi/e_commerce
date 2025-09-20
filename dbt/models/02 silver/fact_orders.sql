{{ config(materialized='table') }}

WITH order_items_agg AS (
    SELECT 
        order_id,
        COUNT(*) as total_items,
        SUM(price) as total_product_value,
        SUM(freight_value) as total_freight_value,
        SUM(price + freight_value) as total_order_value,
        AVG(price) as avg_item_price,
        MAX(price) as max_item_price,
        MIN(price) as min_item_price
    FROM {{ source('raw_data', 'olist_order_items_dataset') }}
    GROUP BY order_id
),

order_payments_agg AS (
    SELECT 
        order_id,
        SUM(payment_value) as total_payment_value,
        COUNT(DISTINCT payment_type) as payment_methods_count,
        STRING_AGG(DISTINCT payment_type, ', ') as payment_types,
        AVG(payment_installments) as avg_installments
    FROM {{ source('raw_data', 'olist_order_payments_dataset') }}
    GROUP BY order_id
)

SELECT 
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp::timestamp as order_purchase_timestamp,
    o.order_approved_at::timestamp as order_approved_at,
    o.order_delivered_carrier_date::timestamp as order_delivered_carrier_date,
    o.order_delivered_customer_date::timestamp as order_delivered_customer_date,
    o.order_estimated_delivery_date::timestamp as order_estimated_delivery_date,
    
    -- Métricas de itens
    oi.total_items,
    oi.total_product_value,
    oi.total_freight_value,
    oi.total_order_value,
    oi.avg_item_price,
    oi.max_item_price,
    oi.min_item_price,
    
    -- Métricas de pagamento
    op.total_payment_value,
    op.payment_methods_count,
    op.payment_types,
    op.avg_installments,
    
    -- Métricas de tempo (em dias)
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_purchase_timestamp IS NOT NULL
        THEN EXTRACT(DAYS FROM (o.order_delivered_customer_date::timestamp - o.order_purchase_timestamp::timestamp))
        ELSE NULL 
    END as delivery_days_actual,
    
    CASE 
        WHEN o.order_estimated_delivery_date IS NOT NULL AND o.order_purchase_timestamp IS NOT NULL
        THEN EXTRACT(DAYS FROM (o.order_estimated_delivery_date::timestamp - o.order_purchase_timestamp::timestamp))
        ELSE NULL 
    END as delivery_days_estimated,
    
    -- Indicadores de qualidade
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_estimated_delivery_date IS NOT NULL
        THEN CASE 
            WHEN o.order_delivered_customer_date::timestamp <= o.order_estimated_delivery_date::timestamp 
            THEN 'On Time' 
            ELSE 'Late' 
        END
        ELSE NULL
    END as delivery_status,
    
    CURRENT_TIMESTAMP as created_at
    
FROM {{ source('raw_data', 'olist_orders_dataset') }} o
LEFT JOIN order_items_agg oi ON o.order_id = oi.order_id  
LEFT JOIN order_payments_agg op ON o.order_id = op.order_id
WHERE o.order_status IS NOT NULL