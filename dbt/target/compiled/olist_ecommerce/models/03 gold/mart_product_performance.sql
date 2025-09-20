

WITH product_sales AS (
    SELECT 
        p.product_id,
        p.category_en as product_category,
        p.category_pt as product_category_pt,
        COUNT(foi.order_item_id) as total_sales,
        SUM(foi.price) as total_revenue,
        SUM(foi.freight_value) as total_freight,
        AVG(foi.price) as avg_price,
        AVG(p.product_weight_g) as avg_weight,
        AVG(p.product_volume_cm3) as avg_volume
    FROM "warehouse"."public"."fact_order_items" foi
    JOIN "warehouse"."public"."dim_products" p ON foi.product_id = p.product_id
    JOIN "warehouse"."public"."fact_orders" o ON foi.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY p.product_id, p.category_en, p.category_pt
)

SELECT 
    ps.product_category,
    COUNT(ps.product_id) as unique_products,
    SUM(ps.total_sales) as total_units_sold,
    SUM(ps.total_revenue) as category_revenue,
    AVG(ps.avg_price) as avg_product_price,
    AVG(ps.avg_weight) as avg_product_weight,
    AVG(ps.avg_volume) as avg_product_volume,
    
    -- Ranking de categorias
    RANK() OVER (ORDER BY SUM(ps.total_revenue) DESC) as revenue_rank,
    RANK() OVER (ORDER BY SUM(ps.total_sales) DESC) as volume_rank,
    
    -- Participação no total (com conversão ::numeric)
    ROUND((SUM(ps.total_revenue) * 100.0 / SUM(SUM(ps.total_revenue)) OVER ())::numeric, 2) as revenue_share_pct,
    
    CURRENT_TIMESTAMP as created_at
FROM product_sales ps
GROUP BY ps.product_category
ORDER BY category_revenue DESC