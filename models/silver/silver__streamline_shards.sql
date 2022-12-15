{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_partition_by_block_number'],
    unique_key = ['shard_id'],
    enabled = true
) }}

WITH shardsjson AS (

    SELECT
        *
    FROM
        {{ ref('silver__load_shards') }}
    WHERE
        {{ incremental_load_filter('_load_timestamp') }}
),
shards AS (
    SELECT
        block_id,
        shard_id,
        VALUE :chunk :: VARIANT AS chunk,
        VALUE :receipt_execution_outcomes :: VARIANT AS receipt_execution_outcomes,
        VALUE :shard_id :: NUMBER AS shard_number,
        VALUE :state_changes :: VARIANT AS state_changes,
        _partition_by_block_number,
        _load_timestamp
    FROM
        shardsjson
)
SELECT
    *
FROM
    shards
