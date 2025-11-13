{{config (
    materialized = 'view',
    tags = 'staging',
    schema = 'dbt_cristian_analytics'
)}}

WITH source AS (
    SELECT * 
    FROM {{source('raw', 'property_listings_raw')}}
),

dropped AS (
    SELECT

        -- ==== safe casting ====
        try_to_decimal(raw_price) as Price,
        try_to_number(raw_living_area) as Living_Area,
        try_to_number(raw_lot_size) as Lot_Size,
        try_to_number(raw_parking) as Parking,
        try_to_number(raw_construction_year) as Construction_Year,
        try_to_date(raw_publish_date) as Publish_Date,
        try_to_number(raw_total_rooms) as Number_Of_Rooms,
        try_to_number(raw_number_of_bedrooms) as Number_Of_Bedrooms,
        try_to_number(raw_number_of_wc) as Number_of_WC, -- this right here is the problem, returns only null values
        try_to_number(raw_number_of_bathrooms) as Number_of_Bathrooms,



        -- ==== Keep and rename ====
        {{title_case('raw_city')}} as City, -- This uses a macro that converts to title case (INITCAP) and also removes trailing or leading spaces (TRIM)
        {{title_case('raw_town')}} as Town,
        {{title_case('raw_district')}} as District,
        {{title_case('raw_type')}} as Category,
        {{title_case('raw_energy_certificate')}} as Energy_Certificate,
        {{title_case('raw_floor')}} as Floor,
        {{title_case('raw_conservation_status')}} as Conversation_Status,

        -- ==== Boolean Flags ====
        raw_has_parking IN ('1', '1.0', 'True') as Has_Parking,
        raw_elevator IN ('1', '1.0', 'True') as Elevator,
        raw_garage IN ('1', '1.0', 'True') as Garage,
        raw_electric_cars_charging IN ('1', '1.0', 'True') Electric_Car_Charge,
    
    FROM source
)


SELECT * FROM dropped

-- this should be strictly renaming and casting columns, because is the first model, raw_Floor will be in another model
-- column names, just add raw and _ after each word
--raw_living_area, raw_lot_size, raw_parking, raw_has_parking, raw_floor, raw_construction_year, raw_publish_date, raw_garage, raw_elevator, raw_electric_car_charge,
--raw_total_rooms, raw_number_of_bedrooms, raw_number_of_wc, raw_conversation_status, raw_number_of_bathrooms