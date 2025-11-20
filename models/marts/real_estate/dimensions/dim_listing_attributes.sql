{{config(materialized='view')}}

select distinct
    {{dbt_utils.generate_surrogate_key([
        'energy_certificate',
        'has_parking',
        'elevator',
        'garage',
        'electric_car_charge'
    ])}} as attribute_key,
    'energy_certificate',
    'has_parking',
    'elevator',
    'garage',
    'electric_car_charge'
from {{ref('int_deduplicated_properties')}}