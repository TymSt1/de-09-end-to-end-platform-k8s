{% macro cents_to_dollars(column_name) %}
    round(({{ column_name }} / 100.0)::numeric, 2)
{% endmacro %}