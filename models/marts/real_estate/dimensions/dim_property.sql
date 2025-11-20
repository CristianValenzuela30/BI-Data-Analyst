{{config(materialized='table')}}

select
    {{dbt_utils.generate_surrogate_key([
        'property_surrogate_key', 'publish_date'
    ])}} as property_key,
    property_surrogate_key,
    publish_date as valid_from,
    coalesce(lead(publish_date) over(partition by property_surrogate_key order by publish_date), '9999-12-31') as valid_to,
    valid_to = '9999-12-31'::date as is_current,
    price,
    living_area,
    lot_size,
    construction_year,
    category,
    floor,
    number_of_bedrooms_imputed as bedrooms,
    number_of_wc_imputed as bathrooms,
    conservation_status
from {{ref('int_deduplicated_properties')}}