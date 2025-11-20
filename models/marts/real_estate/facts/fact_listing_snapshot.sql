{{config(materialized='table')}}

select
    {{dbt_utils.generate_surrogate_key(['property_surrogate_key', 's.publish_date'])}} as fact_key,
    p.property_key,
    l.location_key,
    d.date_key,
    a.attribute_key
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
from {{ref('int_deduplicated_properties')}} as s
left join {{ref("dim_property")}}           as p using (property_surrogate_key, s.publish_date)
left join {{ref('dim_location')}}           as l using (city, town, district)
left join {{ref('dim_date')}}               as d on s.publish_date = d.date_day
left join {{ref('dim_listing_attributes')}} as a
    on s.energy_certificate = a.energy_certificate
    AND s.has_parking = a.has_parking
    AND s.elevator = a.elevator
    AND s.garage = a.garage
    AND s.electric_car_charge = a.electric_car_charge