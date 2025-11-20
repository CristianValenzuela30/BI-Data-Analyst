WITH date_groups AS (
    SELECT
        *,
        -- Calculate base price per date group FIRST
        FIRST_VALUE(price) OVER (
            PARTITION BY 
                district, city, town, category, energy_certificate,
                living_area, lot_size, parking, has_parking, floor,
                construction_year, garage, elevator, electric_car_charge,
                raw_number_of_bedrooms, conservation_status, raw_number_of_wc,
                publish_date
            ORDER BY publish_date DESC
        ) AS group_base_price
    FROM {{ ref('stg_raw_property_listings') }}
),

base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                district, city, town, category, energy_certificate,
                living_area, lot_size, parking, has_parking, floor,
                construction_year, garage, elevator, electric_car_charge,
                raw_number_of_bedrooms, conservation_status, raw_number_of_wc,
                publish_date
            ORDER BY publish_date DESC
        ) AS rn_by_date
    FROM date_groups
),

price_check AS (
    SELECT
        *,
        ABS(price - group_base_price) / NULLIF(group_base_price, 0) AS price_variation
    FROM base
)

SELECT *
FROM price_check
WHERE 
    (price_variation <= 0.08 AND rn_by_date = 1)
    OR 
    (price_variation > 0.08)