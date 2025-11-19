{{config (
    materialized = 'view',
    tags = 'staging',
    schema = 'analytics'
)}}

WITH source AS (
    SELECT * 
    FROM {{source('raw', 'property_listings_raw')}}
),

cleaned_and_imputed AS (
    SELECT

        -- ==== safe casting ====
        try_to_decimal(raw_price) as price, -- Added below to drop where price <= 15K
        try_to_number(raw_living_area) as living_area,
        try_to_number(raw_lot_size) as lot_size,
        try_to_number(raw_parking) as parking,
        try_to_number(raw_construction_year) as construction_year,
        try_to_date(raw_publish_date) as publish_date,
        date(loaded_at) as Loaded_At, -- added loaded at column
        try_to_number(raw_number_of_bedrooms) as bedrooms_raw_temp,

        -- ==== Keep and rename ====
        {{title_case('raw_city')}} as city, -- This uses a macro that converts to title case (INITCAP) and also removes trailing or leading spaces (TRIM)
        {{title_case('raw_town')}} as town,
        {{title_case('raw_district')}} as district,
        {{title_case('raw_type')}} as category,
        {{title_case('raw_energy_certificate')}} as energy_certificate,

        -- ==== Boolean Flags ====
        raw_has_parking IN ('1', '1.0', 'True') as has_parking,
        raw_elevator IN ('1', '1.0', 'True') as elevator,
        raw_garage IN ('1', '1.0', 'True') as garage,
        raw_electric_cars_charging IN ('1', '1.0', 'True') electric_car_charge,

        -- ==== Floor Standardization ====
        CASE
            WHEN UPPER(raw_floor) LIKE '%GROUND FLOOR%' THEN 'Ground Floor'
            WHEN UPPER(raw_floor) LIKE '%1ST FLOOR%' THEN 'First Floor'
            WHEN UPPER(raw_floor) LIKE '%2ND FLOOR%' THEN 'Second Floor'
            WHEN UPPER(raw_floor) LIKE '%3RD FLOOR%' THEN 'Third Floor'
            WHEN UPPER(raw_floor) LIKE '%4TH FLOOR%' THEN 'Fourth Floor'
            WHEN UPPER(raw_floor) LIKE '%5TH FLOOR%' THEN 'Fifth Floor'
            WHEN UPPER(raw_floor) LIKE '%6TH FLOOR%' THEN 'Sixth Floor'
            WHEN UPPER(raw_floor) LIKE '%7TH FLOOR%' THEN 'Seventh Floor'
            WHEN UPPER(raw_floor) LIKE '%8TH FLOOR%' THEN 'Eighth Floor'
            WHEN UPPER(raw_floor) LIKE '%9TH FLOOR%' THEN 'Ninth Floor'
            WHEN UPPER(raw_floor) LIKE '%ABOVE 10TH FLOOR%' THEN 'Above 10th Floor'
            ELSE 'Unknown Floor'
        END AS Floor,



        -- Conservation Status
        COALESCE (
            NULLIF({{title_case('raw_conservation_status')}}, ''), 'Unknown'
        ) AS conservation_status

    FROM source
)

-- Final Selection and Filtering
Select * EXCEPT(bedrooms_raw_temp) FROM cleaned_and_imputed
WHERE 
    -- Critical Filter to eliminate unwanted rows
    Category IN ('Apartment', 'House', 'Land')
    AND city IS NOT NULL
    AND town IS NOT NULL
    AND price > 15000
    AND price IS NOT NULL
    AND (living_area BETWEEN 30 AND 3000 OR living_area IS NULL) 
    AND (lot_size > 100 OR lot_size IS NULL)
    AND (try_to_number(raw_number_of_bedrooms) <= 4 OR raw_number_of_bedrooms IS NULL)