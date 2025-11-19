{% macro impute_bedrooms(category, raw_number_of_bedrooms, raw_number_of_wc, living_area) %}
  CASE
    WHEN {{ category }} = 'Land' THEN 0

    WHEN {{ category }} IN ('Apartment', 'House') AND COALESCE({{ raw_number_of_bedrooms }}, 0) = 0 THEN
      CASE
        WHEN {{ living_area }} IS NULL THEN 1
        WHEN {{ living_area }} < 30 THEN 1 
        WHEN {{ living_area }} < 60 THEN 2
        WHEN {{ living_area }} < 90 THEN 3
        WHEN {{ living_area }} < 120 THEN 4
        ELSE 5
      END
    -- THIS LINE CHANGED: CAP AT 5 EVEN IF RAW VALUE HIGHER  
    ELSE LEAST(COALESCE({{ raw_number_of_bedrooms }}, 0), 5)
  END
{% endmacro %}