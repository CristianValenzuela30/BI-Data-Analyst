{% macro impute_bedrooms_wc(Category, Number_Of_Bedrooms, Number_Of_WC, Living_Area) %}

  CASE
    /* -------------------------------------------------
       1. LAND → 0 / 0
       ------------------------------------------------- */
    WHEN {{ Category }} = 'Land' THEN
      0 AS Bedrooms_Imputed,
      0 AS WC_Imputed

    /* -------------------------------------------------
       2. APARTMENT / HOUSE
       ------------------------------------------------- */
    WHEN {{ Category }} IN ('Apartment', 'House') THEN
      CASE
        /* ---- Both missing → impute from area ---- */
        WHEN COALESCE({{ Number_Of_Bedrooms }}, 0) = 0
         AND COALESCE({{ Number_Of_WC }},       0) = 0 THEN
          CASE
            WHEN {{ Living_Area }} IS NULL THEN
              1 AS Bedrooms_Imputed, 1 AS WC_Imputed
            WHEN {{ Living_Area }} < 30 THEN
              1 AS Bedrooms_Imputed, 1 AS WC_Imputed
            WHEN {{ Living_Area }} < 60 THEN
              1 AS Bedrooms_Imputed, 1 AS WC_Imputed
            WHEN {{ Living_Area }} < 90 THEN
              2 AS Bedrooms_Imputed, 1 AS WC_Imputed
            ELSE
              2 AS Bedrooms_Imputed, 2 AS WC_Imputed
          END

        /* ---- Only bedrooms missing ---- */
        WHEN COALESCE({{ Number_Of_Bedrooms }}, 0) = 0 THEN
          GREATEST(1, FLOOR({{ Living_Area }} / 40)) AS WC_Imputed,
          COALESCE(NULLIF({{ Number_Of_WC }}, 0), 1)  AS Wc_Imputed

        /* ---- Only WC missing ---- */
        WHEN COALESCE({{ Number_Of_WC }}, 0) = 0 THEN
          {{ Number_Of_Bedrooms }}                         AS Bedrooms_Imputed,
          GREATEST(1, FLOOR({{ Number_Of_WC }} / 60))      AS WC_Imputed

        /* ---- Both present → keep original ---- */
        ELSE
          {{ Number_Of_Bedrooms }} AS Bedrooms_Imputed,
          {{ Number_Of_WC }}       AS WC_Imputed
      END

    /* -------------------------------------------------
       3. ANY OTHER TYPE → keep raw
       ------------------------------------------------- */
    ELSE
      {{ Number_Of_Bedroomss }} AS Bedrooms_Imputed,
      {{ Number_Of_WC }}       AS WC_Imputed
  END

{% endmacro %}