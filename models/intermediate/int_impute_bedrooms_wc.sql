{{ config(materialized='view') }}

-- Call the macro and return the two imputed columns
{{ impute_bedrooms_wc(
      Category           = Category,
      Number_Of_Bedrooms = COALESCE(Number_Of_Bedrooms, 0),
      Number_Of_WC       = COALESCE(Number_Of_WC, 0),
      Living_Area        = Living_Area
   )
}}

-- Select everything from the staging model + the imputed columns
SELECT
    *,
    Bedrooms_Imputed AS Number_Of_Bedrooms_Imputed,
    WC_Imputed       AS Number_Of_WC_Imputed
FROM {{ ref('stg_raw_property_listings') }}