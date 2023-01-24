{% macro partition_load_manual(
        scope = 'no_buffer'
    ) %}
    {# if range_start and range_end not set in cli, default to earliest rpc data #}
    {% set range_start = var(
        'range_start',
        46700000
    ) %}
    {% set range_end = var(
        'range_end',
        47000000
    ) %}
    {% set front_buffer = var(
        'front_buffer',
        0
    ) %}
    {% set end_buffer = var(
        'end_buffer',
        0
    ) %}
    {% if scope == 'front' %}
        _partition_by_block_number BETWEEN {{ range_start }} - (
            10000 * {{ front_buffer }}
        )
        AND {{ range_end }}

        {% elif scope == 'end' %}
        _partition_by_block_number BETWEEN {{ range_start }}
        AND {{ range_end }} + (
            10000 * {{ end_buffer }}
        ) {% elif scope == 'no_buffer' %}
        _partition_by_block_number BETWEEN {{ range_start }}
        AND {{ range_end }}
    {% else %}
        TRUE
    {% endif %}
{%- endmacro %}
