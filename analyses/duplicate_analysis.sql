-- Run this analysis first
SELECT 
    property_surrogate_key,
    COUNT(*) as listing_count,
    DATEDIFF('day', MIN(publish_date), MAX(publish_date)) as days_between_listings,
    MAX(price) - MIN(price) as price_difference,
    COUNT(DISTINCT price) as unique_prices
FROM {{ ref('int_impute_bedrooms_wc') }}
GROUP BY property_surrogate_key
HAVING COUNT(*) > 1