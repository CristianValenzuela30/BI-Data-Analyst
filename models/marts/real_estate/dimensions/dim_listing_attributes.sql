{{ config(materialized='view') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'energy_certificate',
        'has_parking', 
        'elevator',
        'garage',
        'electric_car_charge'
    ]) }} AS attribute_key,

    COALESCE(energy_certificate, 'Unknown') as energy_certificate,
    has_parking,
    elevator,
    garage,
    electric_car_charge

FROM {{ ref('int_deduplicated_properties') }}