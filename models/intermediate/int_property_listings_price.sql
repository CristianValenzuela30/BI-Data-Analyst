{{ config(
    materialized = 'ephemeral',
    tags = ['intermediate']
) }}

with staged as (
    select * from {{ ref('stg_raw_property_listings') }}
),

-- Step 1: Calculate median price per group (city, town, type, year)
group_stats as (
    select
        city,
        town,
        property_type,
        construction_year,
        percentile_cont(0.5) within group (order by price_raw) as median_price_group,
        count(*) as group_count
    from staged
    where price_raw > 1000          -- remove obvious junk
      and price_raw is not null
      and city is not null
    group by city, town, property_type, construction_year
    having count(*) >= 5            -- only trust groups with 5+ listings
),

-- Step 2: Join back and impute
with_imputation as (
    select
        s.*,
        coalesce(gs.median_price_group, 250000) as price_imputed,  -- fallback: Portugal avg
        case when s.price_raw <= 1000 or s.price_raw is null then true else false end as was_imputed
    from staged s
    left join group_stats gs
        on s.city = gs.city
       and coalesce(s.town, 'UNKNOWN') = coalesce(gs.town, 'UNKNOWN')
       and s.property_type = gs.property_type
       and coalesce(s.construction_year, 0) = coalesce(gs.construction_year, 0)
),

-- Step 3: Outlier removal (Tukey IQR method per city)
city_iqr as (
    select
        city,
        percentile_cont(0.75) within group (order by price_imputed) as q3,
        percentile_cont(0.25) within group (order by price_imputed) as q1
    from with_imputation
    group by city
),

final as (
    select
        wi.*,
        ci.q3 + 3.0 * (ci.q3 - ci.q1) as upper_fence,
        case 
            when wi.price_imputed > (ci.q3 + 3.0 * (ci.q3 - ci.q1)) then true
            else false
        end as is_outlier
    from with_imputation wi
    left join city_iqr ci on wi.city = ci.city
)

select
    *,
    case 
        when is_outlier then null  -- remove ridiculous prices
        else price_imputed 
    end as price_eur_final
from final