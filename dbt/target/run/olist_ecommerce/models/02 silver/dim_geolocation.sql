
  
    

  create  table "warehouse"."public"."dim_geolocation__dbt_tmp"
  
  
    as
  
  (
    --dim_geolocation



WITH geo_clean AS (
    SELECT DISTINCT
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    FROM "warehouse"."public"."olist_geolocation_dataset"
    WHERE geolocation_lat BETWEEN -35 AND 5  -- Filtrar coordenadas válidas do Brasil
      AND geolocation_lng BETWEEN -75 AND -30
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY geolocation_zip_code_prefix) as geo_id,
    geolocation_zip_code_prefix as zip_code_prefix,
    geolocation_lat as latitude,
    geolocation_lng as longitude,
    geolocation_city as city,
    geolocation_state as state,
    CURRENT_TIMESTAMP as created_at
FROM geo_clean
  );
  