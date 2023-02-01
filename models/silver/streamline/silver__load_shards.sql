{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', '_load_timestamp::DATE'],
    unique_key = 'shard_id',
    full_refresh = False,
    tags = ['s3_load']
) }}

{% set target_partition = 82200000 %}
-- 82200000
-- 82680000

-- split into like 50k at a time

{% set target_partition_low = 82240000 %}
{% set target_partition_high = 82680000 %}


WITH shardsjson AS (

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
        where _partition_by_block_number between {{ target_partition_low }} and {{ target_partition_high }}
        and block_id in (
            select value from NEAR_DEV.tests.chunk_gaps, lateral flatten (blocks_to_walk)
            where _partition_by_block_number between {{ target_partition_low }} and {{ target_partition_high }}
        )
)
SELECT
    *
FROM
    shardsjson
