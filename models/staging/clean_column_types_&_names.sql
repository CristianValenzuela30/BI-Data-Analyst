{{config(
    materialized = 'view',
    tags = ['staging'],
    schema = 'analytics'
)}}

with source as (
    select * from {{source('raw', 'property_listings_raw')}}
),

renamed as (
    select
    -- keys (for grouping later)
    upper(trim(raw_city)) as city,
    upper(trim(raw_town)) as town,
    upper(trim(raw_district)) as district,
    raw_type as property_type,
    try_to_number(raw_construction_year) as construction_year,

    --Price
    try_to_decimal(replace(raw_price, ',', '')) as price_raw,
    raw_price as price_source_value,

    --Areas
    try_to_decimal(raw_gross_area) as gross_area_sqm_raw,
    try_to_decimal(raw_total_area) as total_area_sqm_raw,

    --Booleans
    raw_elevator in ('1', '1.0', 'true') as has_elevator_raw,
    raw_parking in ('1', '1.0', 'true') as has_parking_raw,

    --Other
    try_to_number(raw_number_of_bedrooms) as bedrooms_raw,
    try_to_number(raw_number_of_bathrooms) as bathrooms_raw,
    try_to_date(raw_publish_date) as publish_date,

    loaded_at
from source
)

select * from renamed