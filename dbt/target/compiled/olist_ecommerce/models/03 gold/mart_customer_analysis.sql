

WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_city,
        c.customer_state,
        COUNT(f.order_id) as total_orders,
        SUM(f.total_order_value) as total_spent,
        AVG(f.total_order_value) as avg_order_value,
        MIN(f.order_purchase_timestamp) as first_order_date,
        MAX(f.order_purchase_timestamp) as last_order_date,
        SUM(f.total_items) as total_items_purchased
    FROM "warehouse"."public"."fact_orders" f
    JOIN "warehouse"."public"."dim_customers" c ON f.customer_id = c.customer_id
    WHERE f.order_status = 'delivered'
    GROUP BY c.customer_id, c.customer_city, c.customer_state
),

customer_segments AS (
    SELECT 
        *,
        EXTRACT(DAYS FROM (last_order_date - first_order_date)) as customer_lifetime_days,
        CASE 
            WHEN total_orders = 1 THEN 'One-time'
            WHEN total_orders BETWEEN 2 AND 3 THEN 'Occasional'
            WHEN total_orders BETWEEN 4 AND 8 THEN 'Regular'
            ELSE 'Frequent'
        END as customer_segment,
        
        CASE 
            WHEN total_spent < 100 THEN 'Low Value'
            WHEN total_spent BETWEEN 100 AND 500 THEN 'Medium Value'
            WHEN total_spent BETWEEN 500 AND 1500 THEN 'High Value'
            ELSE 'Premium'
        END as customer_value_segment
    FROM customer_metrics
)

SELECT 
    customer_state,
    customer_segment,
    customer_value_segment,
    COUNT(*) as customer_count,
    SUM(total_spent) as segment_revenue,
    AVG(total_spent) as avg_customer_value,
    AVG(total_orders) as avg_orders_per_customer,
    AVG(avg_order_value) as avg_order_value,
    CURRENT_TIMESTAMP as created_at
FROM customer_segments
GROUP BY customer_state, customer_segment, customer_value_segment
ORDER BY customer_state, segment_revenue DESC