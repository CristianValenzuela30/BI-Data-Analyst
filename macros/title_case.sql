{% macro title_case(column_name) %}
    -- Apply TRIM before applying INITCAP
    INITCAP(TRIM({{ column_name}}))
{% endmacro %}