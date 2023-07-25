{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', '_load_timestamp::DATE'],
    unique_key = 'shard_id',
    full_refresh = False,
    tags = ['load', 'load_shards']
) }}

WITH missing_shards AS (
    -- TODO write a (FAST) ephemeral model to test for shard gaps via missing chunks and add that into the load model
    -- scanning external tables is v slow, but don't just re-load everything in a partition
    SELECT
        DISTINCT _partition_by_block_number
    FROM
        {{ target.database }}.tests.chunk_gaps
),
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
        {% else %}
        WHERE
            {{ partition_batch_load(150000) }}
        {% endif %}
)
SELECT
    *
FROM
    shards_json
