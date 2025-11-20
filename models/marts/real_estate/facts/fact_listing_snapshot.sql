{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['s.property_surrogate_key', 's.publish_date']) }} as fact_key,
    p.property_key,
    l.location_key,
    d.date_key,
    a.attribute_key,
    s.price,
    s.living_area,
    s.lot_size,
    s.construction_year,
    s.bedrooms,
    s.bathrooms,
    s.has_parking,
    s.elevator,
    s.garage,
    s.electric_car_charge,
    s.publish_date,
    s.loaded_at
from {{ ref('int_deduplicated_properties') }} as s
left join {{ ref("dim_property") }} as p 
    on s.property_surrogate_key = p.property_surrogate_key 
    and s.publish_date = p.valid_from  -- FIX: Use valid_from instead of publish_date
left join {{ ref('dim_location') }} as l 
    on s.city = l.city 
    and s.town = l.town 
    and s.district = l.district
left join {{ ref('dim_date') }} as d 
    on s.publish_date = d.date_day
left join {{ ref('dim_listing_attributes') }} as a
    on s.energy_certificate = a.energy_certificate
    and s.has_parking = a.has_parking
    and s.elevator = a.elevator
    and s.garage = a.garage
    and s.electric_car_charge = a.electric_car_charge