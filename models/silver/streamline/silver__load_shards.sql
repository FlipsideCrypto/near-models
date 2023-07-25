{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', '_load_timestamp::DATE'],
    unique_key = 'shard_id',
    full_refresh = False,
    tags = ['load', 'load_shards']
) }}

WITH {% if var("MANUAL_FIX") %}
    missing_shards AS (

        SELECT
            _partition_by_block_number,
            VALUE AS block_id
        FROM
            {{ target.database }}.tests.chunk_gaps,
            LATERAL FLATTEN(
                input => blocks_to_walk
            )
    ),
{% endif %}

shards_json AS (
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

        {% if var("MANUAL_FIX") %}
        WHERE
            _partition_by_block_number IN (
                SELECT
                    DISTINCT _partition_by_block_number
                FROM
                    missing_shards
            )
            AND block_id IN (
                SELECT
                    DISTINCT block_id
                FROM
                    missing_shards
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
