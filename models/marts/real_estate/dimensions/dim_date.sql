{{config(
    materialized='table',
    schema = 'analytics'
)}}

{{ dbt_date.get_date_dimension('2011-07-05', '2030-12-31')}}a