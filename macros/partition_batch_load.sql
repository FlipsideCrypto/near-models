{% macro partition_batch_load(batch_size) %}

{% if is_incremental() %}
WHERE
    _partition_by_block_number BETWEEN (
        SELECT
            MAX(_partition_by_block_number)
        FROM
            {{ this }}
    )
    AND (
        (
            SELECT
                MAX(_partition_by_block_number)
            FROM
                {{ this }}
        ) + {{ batch_size }}
    )
{%- else -%}
WHERE
    _partition_by_block_number BETWEEN 9820000
    AND 10000000
{% endif %}
{%- endmacro %}
