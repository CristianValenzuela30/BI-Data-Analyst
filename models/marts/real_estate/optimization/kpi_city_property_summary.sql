
{{
    config(
        materialized='table',
    )
}}

SELECT
    CITY,
    TOWN,
    DISTRICT,
    COUNT(*) as total_properties,
    AVG(PRICE) as avg_price,
    AVG(LIVING_AREA) as avg_living_area,
    AVG(LOT_SIZE) as avg_lot_size,
    AVG(BEDROOMS) as avg_bedrooms,
    AVG(BATHROOMS) as avg_bathrooms,
    -- Percentage calculations
    SUM(CASE WHEN ELEVATOR = TRUE THEN 1 ELSE 0 END) / COUNT(*) as elevator_pct,
    SUM(CASE WHEN GARAGE = TRUE THEN 1 ELSE 0 END) as garage_count,
    -- Latest data timestamp for freshness tracking
    MAX(PUBLISH_DATE) as last_publish_date,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ ref('property_fact') }} pf
JOIN {{ ref('location_dim') }} l ON pf.LOCATION_KEY = l.LOCATION_KEY
GROUP BY CITY, TOWN, DISTRICT