{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', '_load_timestamp::DATE'],
    unique_key = 'shard_id',
    full_refresh = False,
    tags = ['load', 'load_shards']
) }}

WITH missing_shards AS (

    SELECT
        *
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
            -- TODO can drop this conditional once new process confirmed
        WHERE
            TRUE
            {# _partition_by_block_number IN (
                SELECT
                    DISTINCT _partition_by_block_number
                FROM
                    missing_shards
            ) #}
        {% else %}
        WHERE
            -- TODO change to load timestamp based on s3 file timestamp
            {{ partition_batch_load(150000) }}
            OR -- lookback for late files, should not be needed once changeover to timestamp process
            (
                _partition_by_block_number IN (
                    SELECT
                        DISTINCT _partition_by_block_number
                    FROM
                        missed_shards
                )
                AND block_id IN (
                    SELECT
                        DISTINCT block_id
                    FROM
                        missed_shards
                )
            )
        {% endif %}
)
SELECT
    *
FROM
    shards_json
