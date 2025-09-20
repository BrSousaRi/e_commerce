

SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    CURRENT_TIMESTAMP as created_at
FROM "warehouse"."public"."olist_customers_dataset"