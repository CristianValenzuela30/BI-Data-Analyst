{{ impute_bedrooms_wc(
    category           = 'Category',
    number_of_bedrooms = 'COALESCE(Number_Of_Bedrooms, 0)',
    number_of_wc       = 'COALESCE(Number_Of_WC, 0)',
    living_area        = 'Living_Area'
) }}

SELECT 
    *,
    Bedrooms_Imputed AS Number_Of_Bedrooms_Imputed,
    WC_Imputed       AS Number_Of_WC_Imputed
FROM {{ ref('stg_raw_property_listings') }}