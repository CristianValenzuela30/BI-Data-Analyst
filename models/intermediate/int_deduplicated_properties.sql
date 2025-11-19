{{config(materialized='view')}}

with base_data as (
    select * from {{ref('int_impute_bedrooms_wc')}}
),

ranked_properties as (
    select 
        *,
        row_number() over(partition by 
                            property_surrogate_key 
                            order by 
                            publish_date DESC, loaded_at DESC) as duplicate_rank
    from base_data
)

select * exclude (duplicate_rank)
from ranked_properties
where duplicate_rank = 1