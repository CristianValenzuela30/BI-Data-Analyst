{% macro clean_zero_null_to_unknown(col) %}
    CASE
        WHEN {{col}} IS NULL OR {{col}} = 0 THEN 'Unknown'
        ELSE {{col}}::STRING
    END
{% endmacro %}