{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'chunk_hash',
    cluster_by = ['_inserted_timestamp::date'],
    tags = ['load', 'load_shards']
) }}

WITH shards AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_shards') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
        AND chunk != 'null'
),
FINAL AS (
    SELECT
        block_id,
        shard_id,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp,
        chunk,
        chunk :header :height_created :: NUMBER AS height_created,
        chunk :header :height_included :: NUMBER AS height_included,
        chunk :author :: STRING AS author,
        chunk :header :chunk_hash :: STRING AS chunk_hash,
        chunk :header :: OBJECT AS header,
        chunk :receipts :: ARRAY AS receipts,
        chunk :transactions :: ARRAY AS chunk_transactions
    FROM
        shards
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['chunk_hash']
    ) }} AS streamline_chunks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
