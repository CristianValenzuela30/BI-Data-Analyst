{{
    config(
        materialized='table',
    )
}}

SELECT
    l.CITY,
    l.TOWN,
    l.DISTRICT,
    COUNT(*) as total_properties,
    AVG(pf.PRICE) as avg_price,
    AVG(pf.LIVING_AREA) as avg_living_area,
    AVG(pf.LOT_SIZE) as avg_lot_size,
    AVG(pf.BEDROOMS) as avg_bedrooms,
    AVG(pf.BATHROOMS) as avg_bathrooms,
    -- Percentage calculations
    SUM(CASE WHEN pf.ELEVATOR = TRUE THEN 1 ELSE 0 END) / COUNT(*) as elevator_pct,
    SUM(CASE WHEN pf.GARAGE = TRUE THEN 1 ELSE 0 END) as garage_count,
    -- Latest data timestamp for freshness tracking
    MAX(pf.PUBLISH_DATE) as last_publish_date,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ ref('fact_listing_snapshot') }} pf
JOIN {{ ref('dim_location') }} l ON pf.LOCATION_KEY = l.LOCATION_KEY
GROUP BY l.CITY, l.TOWN, l.DISTRICT