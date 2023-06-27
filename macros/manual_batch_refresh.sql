{% macro partition_load_manual(
        scope = 'no_buffer'
    ) %}
    {# if range_start and range_end not set in cli, default to earliest rpc data #}
    {% set range_start = var(
        'RANGE_START',
        46700000
    ) %}
    {% set range_end = var(
        'RANGE_END',
        47000000
    ) %}
    {% set front_buffer = var(
        'FRONT_BUFFER',
        1
    ) %}
    {% set end_buffer = var(
        'END_BUFFER',
        1
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
