{% macro incremental_load_filter(time_col) -%}
    -- dbt makes it easy to query your target table by using the "{{ this }}" variable.
    {% if is_incremental() %}
      {{ time_col }} > (select max({{ time_col }}) from {{ this }})
    {%- else -%}
      true
    {% endif %}
{%- endmacro %}
