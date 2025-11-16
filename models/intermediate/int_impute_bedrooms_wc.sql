{{ config(materialized='view') }}

{{ impute_bedrooms_wc(
      Category           = Category,
      Number_Of_Bedrooms = {{ coalesce('Number_Of_Bedrooms', '0') }},
      Number_Of_WC       = {{ coalesce('Number_Of_WC', '0') }},
      Living_Area        = Living_Area
   )
}}

SELECT
    *,
    Bedrooms_Imputed AS Number_Of_Bedrooms_Imputed,
    WC_Imputed       AS Number_Of_WC_Imputed
FROM {{ ref('stg_raw_property_listings') }}