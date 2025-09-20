{{ config(materialized='table') }}

SELECT 
    fp.payment_type_clean as payment_method,
    fp.installment_category,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT fp.order_id) as unique_orders,
    SUM(fp.payment_value) as total_payment_value,
    AVG(fp.payment_value) as avg_payment_value,
    AVG(fp.payment_installments) as avg_installments,
    
    -- Participação por método (com conversão ::numeric)
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ())::numeric, 2) as transaction_share_pct,
    ROUND((SUM(fp.payment_value) * 100.0 / SUM(SUM(fp.payment_value)) OVER ())::numeric, 2) as value_share_pct,
    
    -- Análise temporal
    DATE_TRUNC('month', fo.order_purchase_timestamp) as month_year,
    
    CURRENT_TIMESTAMP as created_at
FROM {{ ref('fact_payments') }} fp
JOIN {{ ref('fact_orders') }} fo ON fp.order_id = fo.order_id
WHERE fo.order_status = 'delivered'
    AND fo.order_purchase_timestamp IS NOT NULL
GROUP BY fp.payment_type_clean, fp.installment_category, DATE_TRUNC('month', fo.order_purchase_timestamp)
ORDER BY month_year DESC, total_payment_value DESC