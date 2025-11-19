{{ config(materialized='view') }}

SELECT
    *,
    {{ impute_bedrooms(
        'category',
        'number_of_bedrooms',
        'number_of_wc',
        'living_area'
    ) }} AS number_Of_bedrooms_imputed,

    {{ impute_wc(
        'category',
        'number_of_bedrooms',
        'number_of_wc',
        'living_area'
    )}} AS number_of_wc_imputed
    
FROM {{ ref('stg_raw_property_listings') }}