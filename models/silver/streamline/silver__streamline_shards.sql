{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_partition_by_block_number', '_inserted_timestamp::DATE'],
    unique_key = ['shard_id'],
    tags = ['load', 'load_shards']
) }}

WITH shardsjson AS (

    SELECT
        *
    FROM
        {{ ref('silver__load_shards') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
        qualify ROW_NUMBER() over (
            PARTITION BY shard_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
shards AS (
    SELECT
        block_id,
        shard_id,
        VALUE :chunk :: variant AS chunk,
        VALUE :receipt_execution_outcomes :: variant AS receipt_execution_outcomes,
        VALUE :shard_id :: NUMBER AS shard_number,
        VALUE :state_changes :: variant AS state_changes,
        _partition_by_block_number,
        _load_timestamp,
        _inserted_timestamp
    FROM
        shardsjson
)
SELECT
    *
FROM
    shards
