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
        try_to_decimal(raw_price) as Price, -- Added below to drop where price <= 15K
        try_to_number(raw_living_area) as Living_Area,
        try_to_number(raw_lot_size) as Lot_Size,
        try_to_number(raw_parking) as Parking,
        try_to_number(raw_construction_year) as Construction_Year,
        try_to_date(raw_publish_date) as Publish_Date,
        date(loaded_at) as Loaded_At, -- added loaded at column
        --try_to_number(raw_total_rooms) as Number_Of_Rooms, -- doesnt really add anything, so it will be left out
        try_to_number(raw_number_of_bedrooms) as Number_Of_Bedrooms,
        try_to_number(raw_number_of_wc) as Number_of_WC, 
        try_to_number(raw_number_of_bathrooms) as Number_of_Bathrooms,

        -- ==== Keep and rename ====
        {{title_case('raw_city')}} as City, -- This uses a macro that converts to title case (INITCAP) and also removes trailing or leading spaces (TRIM)
        {{title_case('raw_town')}} as Town,
        {{title_case('raw_district')}} as District,
        {{title_case('raw_type')}} as Category,
        {{title_case('raw_energy_certificate')}} as Energy_Certificate,
        --{{title_case('raw_conservation_status')}} as Conversation_Status,

        -- ==== Boolean Flags ====
        raw_has_parking IN ('1', '1.0', 'True') as Has_Parking,
        raw_elevator IN ('1', '1.0', 'True') as Elevator,
        raw_garage IN ('1', '1.0', 'True') as Garage,
        raw_electric_cars_charging IN ('1', '1.0', 'True') Electric_Car_Charge,

        -- ==== Floor Standardization ====
        CASE
            WHEN UPPER(raw_floor) LIKE '%GROUND FLOOR%' THEN 'Ground Floor'
            WHEN UPPER(raw_floor) LIKE '%1ST FLOOR%' THEN 'First'
            WHEN UPPER(raw_floor) LIKE '%2ND FLOOR%' THEN 'Second'
            WHEN UPPER(raw_floor) LIKE '%3RD FLOOR%' THEN 'Third'
            WHEN UPPER(raw_floor) LIKE '%4TH FLOOR%' THEN 'Fourth'
            WHEN UPPER(raw_floor) LIKE '%5TH FLOOR%' THEN 'Fifth'
            WHEN UPPER(raw_floor) LIKE '%6TH FLOOR%' THEN 'Sixth'
            WHEN UPPER(raw_floor) LIKE '%7TH FLOOR%' THEN 'Seventh'
            WHEN UPPER(raw_floor) LIKE '%8TH FLOOR%' THEN 'Eighth'
            WHEN UPPER(raw_floor) LIKE '%9TH FLOOR%' THEN 'Ninth'
            WHEN UPPER(raw_floor) LIKE '%ABOVE 10TH FLOOR%' THEN 'Above 10th Floor'
            ELSE 'Unknown Floor'
        END AS Floor_Standardized,



        -- Conservation Status
        COALESCE (
            NULLIF({{title_case('raw_conservation_status')}}, ''), 'Unknown'
        ) AS Conservation_Status

    FROM source
)

-- Final Selection and Filtering
Select * FROM cleaned_and_imputed
WHERE 
    -- Critical Filter to eliminate unwanted rows
    Category IN ('Apartment', 'House', 'Land')
    AND City IS NOT NULL
    AND Town IS NOT NULL
    AND Price > 15000
    AND Price IS NOT NULL
    AND (Living_Area BETWEEN 30 AND 3000 OR Living_Area IS NULL) 
    AND (Lot_Size > 100 OR Lot_Size IS NULL)
    AND (Number_Of_Bedrooms <= 4 OR Number_Of_Bedrooms IS NULL)