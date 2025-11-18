{% macro impute_wc(category, number_of_bedrooms, number_of_wc, living_area) %}
  CASE
    WHEN {{ category }} = 'Land' THEN 0
    WHEN {{ category }} IN ('Apartment', 'House') AND COALESCE({{ number_of_wc }}, 0) = 0 THEN
      CASE
        WHEN {{ living_area }} IS NULL THEN 1
        WHEN {{ living_area }} < 30 THEN 1
        WHEN {{ living_area }} < 60 THEN 1
        WHEN {{ living_area }} < 90 THEN 2
        WHEN {{ living_area }} < 120 THEN 2
        WHEN {{ living_area }} < 200 THEN 3
        ELSE 4
      END
    -- THIS LINE CHANGED : CAP AT 4
    ELSE LEAST(COALESCE({{ number_of_wc }}, 0), 4)
  END
{% endmacro %}