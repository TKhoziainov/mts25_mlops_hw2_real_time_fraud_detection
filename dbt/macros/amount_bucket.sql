{% macro amount_bucket(amount_column) %}
  CASE
    WHEN {{ amount_column }} < 10 THEN 'SMALL'
    WHEN {{ amount_column }} < 50 THEN 'MEDIUM'
    WHEN {{ amount_column }} < 200 THEN 'LARGE'
    ELSE 'VERY_LARGE'
  END
{% endmacro %}

