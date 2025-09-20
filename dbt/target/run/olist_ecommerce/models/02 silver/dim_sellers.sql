
  
    

  create  table "warehouse"."public"."dim_sellers__dbt_tmp"
  
  
    as
  
  (
    -- dim_sellers


SELECT 
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    CURRENT_TIMESTAMP as created_at
FROM "warehouse"."public"."olist_sellers_dataset"
  );
  