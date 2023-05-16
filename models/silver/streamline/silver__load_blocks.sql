{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', '_load_timestamp::DATE'],
    unique_key = 'block_id',
    full_refresh = False,
    tags = ['load', 'load_blocks']
) }}

WITH blocks_json AS (

    SELECT
        block_id,
        VALUE,
        _filename,
        _load_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('bronze__streamline_blocks') }}

        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
            AND block_id IN (
                SELECT
                    block_id - 1
                FROM
                    {{ target.database }}.tests.streamline_block_gaps
            )
        {% else %}
        WHERE
            {{ partition_batch_load(150000) }}
        {% endif %}
)
SELECT
    *
FROM
    blocks_json
