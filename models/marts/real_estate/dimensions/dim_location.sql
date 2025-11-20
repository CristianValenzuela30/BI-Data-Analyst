{{config(materialized='view')}}

select distinct
    {{dbt_utils.generate_surrogate_key(['city','town','district'])}} as location_key,
    city,
    town,
    district
from {{ref('int_deduplicated_properties')}}