WITH before_filtering AS (
    SELECT 
        has_parking,
        price_cleaned,
        COUNT(*) as count_before_filter
    FROM cleaned_and_imputed  -- Your CTE before the WHERE clause
    GROUP BY has_parking, price_cleaned
),

after_filtering AS (
    SELECT 
        has_parking,
        COUNT(*) as count_after_filter
    FROM {{ ref('stg_raw_property_listings') }}  -- Your final view
    GROUP BY has_parking
)

SELECT 
    bf.has_parking,
    bf.count_before_filter,
    af.count_after_filter,
    ROUND((bf.count_before_filter - af.count_after_filter) * 100.0 / bf.count_before_filter, 2) as percent_filtered_out
FROM before_filtering bf
LEFT JOIN after_filtering af ON bf.has_parking = af.has_parking
ORDER BY bf.has_parking