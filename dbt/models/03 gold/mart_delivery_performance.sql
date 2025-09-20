{{ config(materialized='table') }}

SELECT 
    c.customer_state,
    f.delivery_status,
    COUNT(*) as order_count,
    AVG(f.delivery_days_actual) as avg_actual_delivery_days,
    AVG(f.delivery_days_estimated) as avg_estimated_delivery_days,
    
    -- Percentis de entrega
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.delivery_days_actual) as median_delivery_days,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.delivery_days_actual) as p90_delivery_days,
    
    -- Taxa de pontualidade
    ROUND((SUM(CASE WHEN f.delivery_status = 'On Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as on_time_delivery_rate,
    
    -- Diferença entre estimado vs real
    AVG(f.delivery_days_actual - f.delivery_days_estimated) as avg_delivery_variance,
    
    CURRENT_TIMESTAMP as created_at
FROM {{ ref('fact_orders') }} f
JOIN {{ ref('dim_customers') }} c ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'
    AND f.delivery_days_actual IS NOT NULL
    AND f.delivery_days_estimated IS NOT NULL
GROUP BY c.customer_state, f.delivery_status
ORDER BY c.customer_state, order_count DESC