{% macro title_case(column_name) %}
    
    INITCAP(TRIM({{ column_name}}))

{% endmacro %}