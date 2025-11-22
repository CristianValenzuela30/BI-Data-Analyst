{{ config(
    materialized='table',
    cluster_by=['city', 'date_key']
    )
 }}

-- NOTE:
-- has_parking excluded from fact model because upstream filters
-- make the column always NULL. If filters change, revisit this.


SELECT
    {{ dbt_utils.generate_surrogate_key(['s.property_surrogate_key', 's.publish_date']) }} AS fact_key,

    p.property_key,
    l.location_key,
    d.date_day AS date_key,
    a.attribute_key,
    s.price,
    s.living_area,
    s.lot_size,
    s.construction_year,
    p.category,
    l.city,
    l.town,
    l.district,
    s.number_of_bedrooms_imputed AS bedrooms,     -- REAL column name
    s.number_of_wc_imputed       AS bathrooms,    -- REAL column name
    s.energy_certificate,
    s.elevator,
    s.garage,
    s.electric_car_charge,
    s.publish_date,

FROM {{ ref('int_deduplicated_properties') }} AS s
LEFT JOIN {{ ref('dim_property') }} AS p
    ON s.property_surrogate_key = p.property_surrogate_key
   AND s.publish_date = p.valid_from
LEFT JOIN {{ ref('dim_location') }} AS l
    USING (city, town, district)
LEFT JOIN {{ ref('dim_date') }} AS d
    ON s.publish_date = d.date_day
LEFT JOIN {{ ref('dim_listing_attributes') }} AS a
    ON COALESCE(s.energy_certificate, 'Unknown')     = a.energy_certificate
   AND COALESCE(s.elevator, False)                   = a.elevator
   AND COALESCE(s.garage, False)                     = a.garage
   AND COALESCE(s.electric_car_charge, False)        = a.electric_car_charge