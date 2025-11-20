WITH base AS (
    SELECT
        *,
        /* -----------------------------------------------
           1. Partition by ALL columns except Price
              (since PublishDate is identical for duplicates)
        ------------------------------------------------ */
        ROW_NUMBER() OVER (
            PARTITION BY
                district, /*-- 18 coolumns in ttoal selected*/
                city,
                town,
                category,
                energy_certificate,
                living_area,
                lot_size,
                parking,
                has_parking,
                floor,
                construction_year,
                garage,
                elevator,
                electric_car_charge,
                raw_number_of_bedrooms,
                conservation_status,
                raw_number_of_wc,
                publish_date
            ORDER BY publish_date DESC   -- doesn't matter, but safe
        ) AS rn_by_date,

        FIRST_VALUE(price) OVER (
            PARTITION BY
                district, /*-- 18 coolumns in ttoal selected*/
                city,
                town,
                category,
                energy_certificate,
                living_area,
                lot_size,
                parking,
                has_parking,
                floor,
                construction_year,
                garage,
                elevator,
                electric_car_charge,
                raw_number_of_bedrooms,
                conservation_status,
                raw_number_of_wc,
                publish_date
            ORDER BY publish_date DESC
        ) AS base_price

    FROM {{ ref('stg_raw_property_listings') }}
),

price_check AS (
    SELECT
        *,
        ABS(price - base_price) / NULLIF(base_price, 0) AS price_variation
    FROM base
),

filtered AS (
    SELECT
        *
    FROM price_check
    WHERE
        /* Rule A: Price difference ≤ 8% → keep only 1 row */
        (price_variation <= 0.08 AND rn_by_date = 1)

        OR

        /* Rule B: Price difference > 8% → keep all (treated as unique) */
        (price_variation > 0.08)
)

SELECT * FROM filtered