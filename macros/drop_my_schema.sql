{% macro drop_my_schema(schema_name) %}
  {% set relation = api.Relation.create(database=target.database, schema=schema_name) %}
  {% do adapter.drop_schema(relation) %}
{% endmacro %}

-- Used to remove the tables created by dbt in snowflake