{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'shard_id',
    tags = ['load', 'load_shards']
) }}

WITH 
load_shards AS (
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
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_shards') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
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
        load_shards
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['shard_id']
    ) }} AS streamline_shards_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    shards
        qualify ROW_NUMBER() over (
            PARTITION BY shard_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
