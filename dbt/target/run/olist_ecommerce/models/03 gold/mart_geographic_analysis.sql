
  
    

  create  table "warehouse"."public"."mart_geographic_analysis__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
    c.customer_state,
    c.customer_city,
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(f.order_id) as total_orders,
    SUM(f.total_order_value) as total_revenue,
    AVG(f.total_order_value) as avg_order_value,
    SUM(f.total_items) as total_items_sold,
    
    -- Métricas por cliente (conversão para NUMERIC)
    ROUND((SUM(f.total_order_value) / COUNT(DISTINCT c.customer_id))::numeric, 2) as revenue_per_customer,
    ROUND((COUNT(f.order_id)::numeric / COUNT(DISTINCT c.customer_id)), 2) as orders_per_customer,
    
    -- Participação no total
    ROUND((SUM(f.total_order_value) * 100.0 / SUM(SUM(f.total_order_value)) OVER ())::numeric, 2) as revenue_share_pct,
    ROUND((COUNT(DISTINCT c.customer_id) * 100.0 / SUM(COUNT(DISTINCT c.customer_id)) OVER ())::numeric, 2) as customer_share_pct,
    
    CURRENT_TIMESTAMP as created_at
FROM "warehouse"."public"."dim_customers" c
JOIN "warehouse"."public"."fact_orders" f ON c.customer_id = f.customer_id
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state, c.customer_city
HAVING COUNT(f.order_id) >= 10
ORDER BY total_revenue DESC
  );
  