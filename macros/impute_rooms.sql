{# macros/impute_bedrooms_wc.sql #}
{% macro impute_bedrooms_wc(property_type, number_of_bedrooms, number_of_wc, living_area) %}

  CASE
    /* -------------------------------------------------
       1. LAND → everything is 0
       ------------------------------------------------- */
    WHEN LOWER({{ property_type }}) = 'land' THEN
      0 AS bedrooms_imputed,
      0 AS wc_imputed

    /* -------------------------------------------------
       2. APARTMENT / HOUSE
       ------------------------------------------------- */
    WHEN LOWER({{ property_type }}) IN ('apartment', 'house') THEN
      CASE
        /* ---- Both bedrooms & WC are missing (0 or NULL) ---- */
        WHEN COALESCE({{ number_of_bedrooms }}, 0) = 0
         AND COALESCE({{ number_of_wc }},       0) = 0 THEN
          CASE
            WHEN {{ living_area }} IS NULL THEN
              1 AS bedrooms_imputed, 1 AS wc_imputed      -- safe default
            WHEN {{ living_area }} < 30 THEN
              1 AS bedrooms_imputed, 1 AS wc_imputed      -- studio
            WHEN {{ living_area }} < 60 THEN
              1 AS bedrooms_imputed, 1 AS wc_imputed      -- 1-bed
            WHEN {{ living_area }} < 90 THEN
              2 AS bedrooms_imputed, 1 AS wc_imputed      -- 2-bed
            ELSE
              2 AS bedrooms_imputed, 2 AS wc_imputed      -- larger
          END

        /* ---- Only bedrooms missing ---- */
        WHEN COALESCE({{ number_of_bedrooms }}, 0) = 0 THEN
          GREATEST(1, FLOOR({{ living_area }} / 40)) AS bedrooms_imputed,
          COALESCE(NULLIF({{ number_of_wc }}, 0), 1)  AS wc_imputed

        /* ---- Only WC missing ---- */
        WHEN COALESCE({{ number_of_wc }}, 0) = 0 THEN
          {{ number_of_bedrooms }}                         AS bedrooms_imputed,
          GREATEST(1, FLOOR({{ living_area }} / 60))      AS wc_imputed

        /* ---- Both present → keep them ---- */
        ELSE
          {{ number_of_bedrooms }} AS bedrooms_imputed,
          {{ number_of_wc }}       AS wc_imputed
      END

    /* -------------------------------------------------
       3. ANY OTHER TYPE → keep raw values
       ------------------------------------------------- */
    ELSE
      {{ number_of_bedrooms }} AS bedrooms_imputed,
      {{ number_of_wc }}       AS wc_imputed
  END

{% endmacro %}