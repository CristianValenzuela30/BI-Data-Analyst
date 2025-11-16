{{ config(materialized='view') }}

SELECT
    *,
    {{ impute_bedrooms_wc(
        Category='Category',
        Number_Of_Bedrooms='Number_Of_Bedrooms',
        Number_Of_WC='Number_Of_WC',
        Living_Area='Living_Area'
    ) }},
    Bedrooms_Imputed AS Number_Of_Bedrooms_Imputed,
    WC_Imputed       AS Number_Of_WC_Imputed
FROM {{ ref('stg_raw_property_listings') }}
