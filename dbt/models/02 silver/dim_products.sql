{{ config(materialized='table') }}

-- dim_products.sql
SELECT 
    p.product_id,
    p.product_category_name as category_pt,
    COALESCE(t.product_category_name_english, 'Unknown') as category_en,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    -- Calcular volume do produto
    CASE 
        WHEN p.product_length_cm > 0 AND p.product_height_cm > 0 AND p.product_width_cm > 0
        THEN p.product_length_cm * p.product_height_cm * p.product_width_cm
        ELSE NULL
    END as product_volume_cm3,
    CURRENT_TIMESTAMP as created_at
FROM {{ source('raw_data', 'olist_products_dataset') }} p
LEFT JOIN {{ source('raw_data', 'product_category_name_translation') }} t
    ON p.product_category_name = t.product_category_name