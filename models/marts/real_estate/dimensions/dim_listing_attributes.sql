{{config(materialized='view')}}

with distinct_attributes as (
    select distinct
        COALESCE(energy_certificate, 'Unknown') as energy_certificate,
        COALESCE(has_parking, False) as has_parking,
        COALESCE(elevator, False) as elevator,
        COALESCE(garage, False) as garage,
        COALESCE(electric_car_charge, False) as electric_car_charge
    from {{ref('int_deduplicated_properties')}}
)
select
    {{dbt_utils.generate_surrogate_key([
        'energy_certificate',
        'has_parking',
        'elevator',
        'garage',
        'electric_car_charge'
    ])}} as attribute_key,
    energy_certificate,
    has_parking,
    elevator,
    garage,
    electric_car_charge
from distinct_attributes