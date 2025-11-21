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

    -- Create Surrogate Key using Multiple columns (recommended)
    {{ dbt_utils.generate_surrogate_key([
    'city',
    'town', 
    'district',
    'category',
    'conservation_status',
    --'has_parking',
    'elevator',
    'garage',           
    'living_area',        
    'lot_size',           
    'construction_year',  
    'floor',              
    'number_of_bedrooms_imputed',  
    'number_of_wc_imputed',     
    ]) }} AS property_surrogate_key

FROM {{ ref('stg_raw_property_listings') }}