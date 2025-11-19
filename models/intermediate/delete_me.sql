SELECT *
FROM {{ ref('int_impute_bedrooms_wc') }}
WHERE property_surrogate_key = 'afd5b35893007fd0862b8ba67eb7f8b0'  -- Replace with actual duplicate key
ORDER BY publish_date DESC, loaded_at DESC