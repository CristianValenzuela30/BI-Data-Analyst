{{config(materialized='view')}}

/* step 1: average the price when the same property has the same publish date */
with with_avg_price as (
    select
        *,
        avg(price) over(
            partition by
                property_surrogate_key, publish_date
        ) as price_avg_same_day
    from {{ref('int_impute_bedrooms_wc')}}
),

/* Step 2:  deduplicate - keep only the most recent scrape of that day */

deduplication as (
    select *
    from (
        select
            *,
            row_number() over(
                partition by property_surrogate_key, publish_date
                order by loaded_at DESC
            ) as rn
        from with_avg_price
    ) as ranked
    where rn = 1
)

/* final output; clean, one row per property per publish_date with avgeraged price */

select
    * EXCLUDE(rn, price_avg_same_day),
    price_avg_same_day -- as price -- renamed price back to 'price'
from deduplication