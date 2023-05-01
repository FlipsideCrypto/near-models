{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', '_load_timestamp::DATE'],
    unique_key = 'shard_id',
    full_refresh = False,
    tags = ['load', 'load_shards']
) }}

WITH shards_json AS (

    SELECT
        block_id,
        concat_ws(
            '-',
            block_id :: STRING,
            _shard_number :: STRING
        ) AS shard_id,
        _shard_number,
        VALUE,
        _filename,
        _load_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('bronze__streamline_shards') }}

        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
            AND block_id IN (
                SELECT
                    VALUE
                FROM
                    {{ target.database }}.tests.chunk_gaps,
                    LATERAL FLATTEN(
                        input => blocks_to_walk
                    )
            )
        {% else %}
        WHERE
            {{ partition_batch_load(150000) }}
        {% endif %}
)
SELECT
    *
FROM
    shards_json
