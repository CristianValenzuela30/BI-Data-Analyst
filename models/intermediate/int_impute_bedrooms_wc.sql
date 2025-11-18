{{ config(materialized='view') }}

SELECT
    *,
    {{ impute_bedrooms(
        'Category',
        'Number_Of_Bedrooms',
        'Number_Of_WC',
        'Living_Area'
    ) }} AS Number_Of_Bedrooms_Imputed,

    {{ impute_wc(
        'Category',
        'Number_Of_Bedrooms',
        'Number_Of_WC',
        'Living_Area'
    )}} AS Number_Of_WC_Imputed
    
FROM {{ ref('stg_raw_property_listings') }}