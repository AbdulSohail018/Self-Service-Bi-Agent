{% macro datediff(start_date, end_date, unit) %}
  {{ return(adapter.dispatch('datediff', 'dbt')(start_date, end_date, unit)) }}
{% endmacro %}

{% macro default__datediff(start_date, end_date, unit) %}
  {% if unit == 'day' %}
    datediff('day', {{ start_date }}, {{ end_date }})
  {% elif unit == 'month' %}
    datediff('month', {{ start_date }}, {{ end_date }})
  {% elif unit == 'year' %}
    datediff('year', {{ start_date }}, {{ end_date }})
  {% else %}
    datediff('{{ unit }}', {{ start_date }}, {{ end_date }})
  {% endif %}
{% endmacro %}

{% macro duckdb__datediff(start_date, end_date, unit) %}
  {% if unit == 'day' %}
    ({{ end_date }} - {{ start_date }})::integer
  {% elif unit == 'month' %}
    datediff('month', {{ start_date }}, {{ end_date }})
  {% elif unit == 'year' %}
    datediff('year', {{ start_date }}, {{ end_date }})
  {% else %}
    datediff('{{ unit }}', {{ start_date }}, {{ end_date }})
  {% endif %}
{% endmacro %}

{% macro snowflake__datediff(start_date, end_date, unit) %}
  datediff({{ unit }}, {{ start_date }}, {{ end_date }})
{% endmacro %}

{% macro bigquery__datediff(start_date, end_date, unit) %}
  {% if unit == 'day' %}
    date_diff({{ end_date }}, {{ start_date }}, day)
  {% elif unit == 'month' %}
    date_diff({{ end_date }}, {{ start_date }}, month)
  {% elif unit == 'year' %}
    date_diff({{ end_date }}, {{ start_date }}, year)
  {% else %}
    date_diff({{ end_date }}, {{ start_date }}, {{ unit }})
  {% endif %}
{% endmacro %}

{% macro get_current_timestamp() %}
  {{ return(adapter.dispatch('get_current_timestamp', 'dbt')()) }}
{% endmacro %}

{% macro default__get_current_timestamp() %}
  current_timestamp()
{% endmacro %}

{% macro duckdb__get_current_timestamp() %}
  current_timestamp
{% endmacro %}

{% macro snowflake__get_current_timestamp() %}
  current_timestamp()
{% endmacro %}

{% macro bigquery__get_current_timestamp() %}
  current_timestamp()
{% endmacro %}

{% macro date_trunc(period, date_expr) %}
  {{ return(adapter.dispatch('date_trunc', 'dbt')(period, date_expr)) }}
{% endmacro %}

{% macro default__date_trunc(period, date_expr) %}
  date_trunc('{{ period }}', {{ date_expr }})
{% endmacro %}

{% macro duckdb__date_trunc(period, date_expr) %}
  date_trunc('{{ period }}', {{ date_expr }})
{% endmacro %}

{% macro snowflake__date_trunc(period, date_expr) %}
  date_trunc({{ period }}, {{ date_expr }})
{% endmacro %}

{% macro bigquery__date_trunc(period, date_expr) %}
  {% if period == 'week' %}
    date_trunc({{ date_expr }}, week)
  {% elif period == 'month' %}
    date_trunc({{ date_expr }}, month)
  {% elif period == 'quarter' %}
    date_trunc({{ date_expr }}, quarter)
  {% elif period == 'year' %}
    date_trunc({{ date_expr }}, year)
  {% else %}
    date_trunc({{ date_expr }}, {{ period }})
  {% endif %}
{% endmacro %}