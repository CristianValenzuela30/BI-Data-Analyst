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
        try_to_decimal(raw_price) as Price,
        try_to_number(raw_living_area) as Living_Area,
        try_to_number(raw_lot_size) as Lot_Size,
        try_to_number(raw_parking) as Parking,
        try_to_number(raw_construction_year) as Construction_Year,
        try_to_date(raw_publish_date) as Publish_Date,
        date(loaded_at) as Loaded_At, -- added loaded at column
        try_to_number(raw_total_rooms) as Number_Of_Rooms,
        try_to_number(raw_number_of_bedrooms) as Number_Of_Bedrooms,
        try_to_number(raw_number_of_wc) as Number_of_WC, 
        try_to_number(raw_number_of_bathrooms) as Number_of_Bathrooms,

        -- ==== Keep and rename ====
        {{title_case('raw_city')}} as City, -- This uses a macro that converts to title case (INITCAP) and also removes trailing or leading spaces (TRIM)
        {{title_case('raw_town')}} as Town,
        {{title_case('raw_district')}} as District,
        {{title_case('raw_type')}} as Category,
        {{title_case('raw_energy_certificate')}} as Energy_Certificate,
        {{title_case('raw_conservation_status')}} as Conversation_Status,


        -- ==== Boolean Flags ====
        raw_has_parking IN ('1', '1.0', 'True') as Has_Parking,
        raw_elevator IN ('1', '1.0', 'True') as Elevator,
        raw_garage IN ('1', '1.0', 'True') as Garage,
        raw_electric_cars_charging IN ('1', '1.0', 'True') Electric_Car_Charge,

        -- ==== Floor Standardization ====
        CASE
            WHEN Category IN ('House', 'Land') THEN 'GROUND FLOOR'
            WHEN Category = 'Apartment' THEN
                CASE
                    WHEN {{title_case('raw_floor')}} IN ('1st Floor', 'First Floor', 'Primero Piso') THEN 'First Floor'
                    WHEN {{title_case('raw_floor')}} IN ('2nd Floor', 'Second Floor') THEN 'Second Floor'
                    WHEN {{title_case('raw_floor')}} IN ('3rd Floor', 'Third Floor') THEN 'Third Floor'
                    WHEN {{title_case('raw_floor')}} IN ('4th Floor', 'Fourth Floor') THEN '4th to 6th Floor'
                    WHEN {{title_case('raw_floor')}} IN ('5th Floor', 'Fifth Floor') THEN '4th to 6th Floor'
                    WHEN {{title_case('raw_floor')}} IN ('6th Floor', 'Sixth Floor') THEN '4th to 6th Floor'
                    WHEN {{title_case('raw_floor')}} IN ('7th Floor', 'Seventh Floor') THEN '7th to 10th Floor'
                    WHEN {{title_case('raw_floor')}} IN ('8th Floor', 'Eighth Floor') THEN '7th to 10th Floor'
                    WHEN {{title_case('raw_floor')}} IN ('9th Floor', 'Ninth Floor') THEN '7th to 10th Floor'
                    WHEN {{title_case('raw_floor')}} IN ('10th Floor', 'Tenth Floor') THEN '7th to 10th Floor'
                    WHEN {{title_case('raw_floor')}} IN ('Ground Floor', 'Bajo', 'Basement', 'Basement Level', 'Attic') THEN 'GROUND FLOOR'
                    WHEN {{title_case('raw_floor')}} IN ('Top Floor', 'Ático', 'Penthouse') THEN 'Above 10th Floor'
                    ELSE 'Unknown Floor'
                END
            ELSE 'Not Applicable'
        END AS Floor_Standardized

    FROM source
)

-- Final Selection and Filtering
Select * FROM cleaned_and_imputed
WHERE 
    -- Critical Filter to eliminate unwanted rows
    Category IN ('Apartment', 'House', 'Land')
