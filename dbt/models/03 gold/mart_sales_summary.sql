{{ config(materialized='table') }}

SELECT 
    DATE_TRUNC('month', order_purchase_timestamp) as month_year,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_order_value) as total_revenue,
    AVG(total_order_value) as avg_order_value,
    SUM(total_items) as total_items_sold,
    AVG(delivery_days_actual) as avg_delivery_days,
    
    -- Métricas de crescimento
    LAG(COUNT(DISTINCT order_id)) OVER (ORDER BY DATE_TRUNC('month', order_purchase_timestamp)) as prev_month_orders,
    LAG(SUM(total_order_value)) OVER (ORDER BY DATE_TRUNC('month', order_purchase_timestamp)) as prev_month_revenue,
    
    -- Calcular crescimento percentual (com conversão ::numeric)
    CASE 
        WHEN LAG(COUNT(DISTINCT order_id)) OVER (ORDER BY DATE_TRUNC('month', order_purchase_timestamp)) > 0
        THEN ROUND(((COUNT(DISTINCT order_id) - LAG(COUNT(DISTINCT order_id)) OVER (ORDER BY DATE_TRUNC('month', order_purchase_timestamp))) * 100.0 / LAG(COUNT(DISTINCT order_id)) OVER (ORDER BY DATE_TRUNC('month', order_purchase_timestamp)))::numeric, 2)
        ELSE NULL
    END as orders_growth_pct,
    
    CASE 
        WHEN LAG(SUM(total_order_value)) OVER (ORDER BY DATE_TRUNC('month', order_purchase_timestamp)) > 0
        THEN ROUND(((SUM(total_order_value) - LAG(SUM(total_order_value)) OVER (ORDER BY DATE_TRUNC('month', order_purchase_timestamp))) * 100.0 / LAG(SUM(total_order_value)) OVER (ORDER BY DATE_TRUNC('month', order_purchase_timestamp)))::numeric, 2)
        ELSE NULL
    END as revenue_growth_pct,
    
    CURRENT_TIMESTAMP as created_at
FROM {{ ref('fact_orders') }}
WHERE order_status = 'delivered'
    AND order_purchase_timestamp IS NOT NULL
GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
ORDER BY month_year