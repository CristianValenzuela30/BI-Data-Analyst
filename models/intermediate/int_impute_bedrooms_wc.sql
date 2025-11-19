{{ config(materialized='view') }}

SELECT
    *,
    {{ impute_bedrooms(
        'category',
        'raw_number_of_bedrooms',
        'raw_number_of_wc',
        'living_area'
    ) }} AS number_of_bedrooms_imputed,

    {{ impute_wc(
        'category',
        'raw_number_of_bedrooms',
        'raw_number_of_wc',
        'living_area'
    )}} AS number_of_wc_imputed,

    -- Create Surrogate Key using 5 columns
    {{ dbt_utils.generate_surrogate_key([
        'city',
        'town',
        'district',
        'living_area',
        'construction_year',
        'category'
    ]) }} as property_surrogate_key
    
FROM {{ ref('stg_raw_property_listings') }}