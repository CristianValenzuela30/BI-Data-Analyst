{% macro impute_wc(category, number_of_bedrooms, number_of_wc, living_area) %}
  CASE
    WHEN {{ category }} = 'Land' THEN 0

    -- Handle all invalid values: NULL, 0, Negative, or unreasonably high
    WHEN {{ category }} IN ('Apartment', 'House') AND
        ({{ number_of_wc }} IS NULL OR
        {{number_of_wc }} <= 0
        {{ number_of_wc }} > 10) THEN -- Also Catch unreasonably high values
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