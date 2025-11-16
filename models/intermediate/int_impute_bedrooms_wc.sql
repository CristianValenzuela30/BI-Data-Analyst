{{impute_bedrooms_wc(
    Category,
    Number_Of_Bedrooms = 'COALESCE(Number_Of_Bedrooms, 0)',
    Number_Of_WC = 'COALESCE(Number_Of_WC, 0)',
    Living_Area = 'Living_Area'
)}}
SELECT *, Bedrooms_Imputed as Number_Of_Bedrooms_Imputed,
WC_Imputed as Number_Of_WC_Imputed
FROM {{ref('stg_raw_property_listings')}}