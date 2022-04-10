{% macro incremental_load_filter(time_col) -%}
    -- dbt makes it easy to query your target table by using the "{{ this }}" variable.
    {% if is_incremental() %}
      {{ time_col }} > (select max({{ time_col }}) from {{ this }})
    {%- else -%}
      true
    {% endif %}
{%- endmacro %}

{% macro incremental_last_x_days(time_col, time_in_days) -%}
    {% if is_incremental() %}
        {{ time_col }} >= current_date() - interval '{{ time_in_days }} day'
    {% else %}
        true
    {% endif %}
{%- endmacro %}
