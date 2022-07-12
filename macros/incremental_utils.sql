{% macro incremental_load_filter(time_col) -%}
  -- dbt makes it easy to query your target table by using the "{{ this }}" variable.

{% if is_incremental() %}
{{ time_col }} >= (
  SELECT
    MAX(
      {{ time_col }}
    )
  FROM
    {{ this }}
)
{%- else -%}
  TRUE
{% endif %}
{%- endmacro %}

{% macro incremental_last_x_days(
    time_col,
    time_in_days
  ) -%}

{% if is_incremental() %}
{{ time_col }} >= CURRENT_DATE() - INTERVAL '{{ time_in_days }} day'
{% else %}
  TRUE
{% endif %}
{%- endmacro %}
