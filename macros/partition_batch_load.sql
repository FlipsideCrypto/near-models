{% macro partition_batch_load(batch_size) %}

{% if is_incremental() %}
_partition_by_block_number BETWEEN (
    SELECT
        MAX(_partition_by_block_number) - 10000
    FROM
        {{ this }}
)
AND (
    (
        SELECT
            MAX(_partition_by_block_number) - 10000
        FROM
            {{ this }}
    ) + {{ batch_size }}
)
{%- else -%}
    _partition_by_block_number BETWEEN 9820000
    AND 10000000
{% endif %}
{%- endmacro %}

{% macro partition_incremental_load(
        batch_size,
        front_buffer = 0,
        end_buffer = 0
    ) %}

{% if is_incremental() %}
_partition_by_block_number BETWEEN (
    SELECT
        MAX(_partition_by_block_number) - {{ front_buffer }}
    FROM
        {{ this }}
)
AND (
    (
        SELECT
            MAX(_partition_by_block_number)
        FROM
            {{ this }}
    ) + {{ batch_size }} + {{ end_buffer }}
)
{%- else -%}
    TRUE
{% endif %}
{%- endmacro %}
