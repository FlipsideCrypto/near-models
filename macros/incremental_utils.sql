-- TODO slated for deprecation and drop
{% macro incremental_load_filter(time_col) -%}

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
{{ time_col }} :: DATE >= SYSDATE() - INTERVAL '{{ time_in_days }}'
{% else %}
  TRUE
{% endif %}
{%- endmacro %}

{% macro incremental_pad_x_minutes(
    time_col,
    time_in_minutes
  ) -%}

{% if is_incremental() %}
{{ time_col }} >= (
  SELECT
    MAX(
      {{ time_col }}
    )
  FROM
    {{ this }}
) - INTERVAL '{{ time_in_minutes }} minutes'
{% else %}
  TRUE
{% endif %}
{%- endmacro %}
