{{ config(materialized='view') }}

SELECT
    *,
    {{ impute_bedrooms(
        'Category',
        'Number_Of_Bedrooms', 
        'Number_Of_WC',
        'Living_Area'
    ) }} as Number_Of_Bedrooms_Imputed,
    
    {{ impute_wc(
        'Category', 
        'Number_Of_Bedrooms',
        'Number_Of_WC',
        'Living_Area'
    ) }} as Number_Of_WC_Imputed

FROM {{ ref('stg_raw_property_listings') }}
-- Add filters to exclude impossible values
WHERE Number_Of_WC BETWEEN 0 AND 10  -- WC should be between 0-10
  AND Number_Of_Bedrooms BETWEEN 0 AND 10  -- Bedrooms should be reasonable too