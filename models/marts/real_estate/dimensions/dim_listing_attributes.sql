{{ config(materialized='view') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'COALESCE(energy_certificate, ''Unknown'')',
        'COALESCE(has_parking::TEXT, ''False'')',
        'COALESCE(elevator::TEXT, ''False'')',
        'COALESCE(garage::TEXT, ''False'')',
        'COALESCE(electric_car_charge::TEXT, ''False'')'
    ]) }} AS attribute_key,

    energy_certificate,
    has_parking,
    elevator,
    garage,
    electric_car_charge

FROM {{ ref('int_deduplicated_properties') }}
