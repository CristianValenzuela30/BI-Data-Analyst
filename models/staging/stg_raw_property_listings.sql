{{
    config(
        materialized = 'view',
        tags = 'staging',
        schema = 'analytics'
    )
}}

WITH source AS (
    SELECT * 
    FROM {{ source('raw', 'property_listings_raw') }}
),

cleaned_and_imputed AS (
    SELECT
        -- ==== Primary Key & Identifiers ====
        {{ dbt_utils.generate_surrogate_key([
            'raw_city', 'raw_town', 'raw_district', 'raw_type',
            'raw_living_area', 'raw_lot_size', 'raw_construction_year',
            'raw_floor', 'raw_number_of_bedrooms', 'raw_number_of_wc',
            'raw_publish_date'
        ]) }} AS source_surrogate_key,

        -- ==== Price & Financials (with validation) ====
        try_to_decimal(raw_price) as price,
        CASE 
            WHEN try_to_decimal(raw_price) BETWEEN 15000 AND 10000000 THEN try_to_decimal(raw_price)
            ELSE NULL  -- Flag extreme prices as NULL for later handling
        END AS price_cleaned,

        -- ==== Area Metrics (with validation) ====
        CASE 
            WHEN try_to_number(raw_living_area) BETWEEN 30 AND 3000 THEN try_to_number(raw_living_area)
            ELSE NULL
        END AS living_area,
        
        CASE 
            WHEN try_to_number(raw_lot_size) > 100 THEN try_to_number(raw_lot_size)
            ELSE NULL
        END AS lot_size,

        try_to_number(raw_parking) as parking,

        -- ==== Construction Year (with validation) ====
        CASE 
            WHEN try_to_number(raw_construction_year) BETWEEN 1800 AND YEAR(CURRENT_DATE()) + 5 
            THEN try_to_number(raw_construction_year)
            ELSE NULL
        END AS construction_year,

        -- ==== Dates ====
        try_to_date(raw_publish_date) as publish_date,
        date(loaded_at) as loaded_at,

        -- ==== Bedrooms & Bathrooms (with imputation) ====
        COALESCE(try_to_number(raw_number_of_bedrooms), 0) as raw_number_of_bedrooms,
        COALESCE(try_to_number(raw_number_of_wc), 0) as raw_number_of_wc,

        -- ==== Geographic Data (cleaned and standardized) ====
        {{ title_case('raw_city') }} as city,
        {{ title_case('raw_town') }} as town,
        {{ title_case('raw_district') }} as district,
        {{ title_case('raw_type') }} as category,

        -- ==== Energy Certificate (with complete standardization) ====
        CASE 
            WHEN NULLIF({{ title_case('raw_energy_certificate') }}, '') IS NULL THEN 'Unknown'
            WHEN UPPER({{ title_case('raw_energy_certificate') }}) IN ('NC', 'N/C', 'NOT CERTIFIED') THEN 'Nc'
            WHEN UPPER({{ title_case('raw_energy_certificate') }}) = 'NO CERTIFICATE' THEN 'No Certificate'
            ELSE {{ title_case('raw_energy_certificate') }}
        END AS energy_certificate,

        -- ==== Boolean Flags (with NULL handling) ====
        RAW_HAS_PARKING = 'True' as has_parking,
        raw_elevator = 'True' as elevator,
        raw_garage = 'True' as garage,
        raw_electric_cars_charging = 'True' as electric_car_charge,

        -- ==== Floor (standardized with NULL handling) ====
        CASE
            WHEN UPPER(TRIM(raw_floor)) LIKE '%GROUND FLOOR%' THEN 'Ground Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%1ST FLOOR%' OR UPPER(TRIM(raw_floor)) = '1' THEN 'First Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%2ND FLOOR%' OR UPPER(TRIM(raw_floor)) = '2' THEN 'Second Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%3RD FLOOR%' OR UPPER(TRIM(raw_floor)) = '3' THEN 'Third Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%4TH FLOOR%' OR UPPER(TRIM(raw_floor)) = '4' THEN 'Fourth Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%5TH FLOOR%' OR UPPER(TRIM(raw_floor)) = '5' THEN 'Fifth Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%6TH FLOOR%' OR UPPER(TRIM(raw_floor)) = '6' THEN 'Sixth Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%7TH FLOOR%' OR UPPER(TRIM(raw_floor)) = '7' THEN 'Seventh Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%8TH FLOOR%' OR UPPER(TRIM(raw_floor)) = '8' THEN 'Eighth Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%9TH FLOOR%' OR UPPER(TRIM(raw_floor)) = '9' THEN 'Ninth Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%10%' OR UPPER(TRIM(raw_floor)) = '10' THEN 'Tenth Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%ABOVE 10%' THEN 'Above 10th Floor'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%BASEMENT%' THEN 'Basement'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%MEZZANINE%' THEN 'Mezzanine'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%ATTIC%' THEN 'Attic'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%DUPLEX%' THEN 'Duplex'
            WHEN UPPER(TRIM(raw_floor)) LIKE '%TRIPLEX%' THEN 'Triplex'
            ELSE 'Unknown Floor'
        END AS floor,

        -- ==== Conservation Status ====
        COALESCE(NULLIF({{ title_case('raw_conservation_status') }}, ''), 'Unknown') AS conservation_status,

        -- ==== Data Quality Flags ====
        CASE WHEN try_to_decimal(raw_price) IS NULL THEN 1 ELSE 0 END AS is_price_invalid,
        CASE WHEN try_to_number(raw_living_area) IS NULL THEN 1 ELSE 0 END AS is_living_area_invalid,
        CASE WHEN try_to_date(raw_publish_date) IS NULL THEN 1 ELSE 0 END AS is_publish_date_invalid,
        CASE WHEN {{ title_case('raw_city') }} IS NULL THEN 1 ELSE 0 END AS is_city_missing

    FROM source
)

-- Final Selection with Business Logic Filtering
SELECT 
    *,
    -- Overall data quality score
    (is_price_invalid + is_living_area_invalid + is_publish_date_invalid + is_city_missing) AS data_quality_score
    
FROM cleaned_and_imputed
WHERE 
    -- Business Logic Filters
    Category IN ('Apartment', 'House', 'Land')
    AND city IS NOT NULL
    AND town IS NOT NULL
    AND price_cleaned IS NOT NULL  -- Use the cleaned price
    AND publish_date IS NOT NULL
    -- Additional quality threshold
    AND data_quality_score <= 2  -- Allow some minor data issues