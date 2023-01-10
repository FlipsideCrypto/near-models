{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'chunk_hash',
    cluster_by = ['_load_timestamp::date','height_created','height_included']
) }}

WITH shards AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_shards') }}
    WHERE
        chunk != 'null'
        AND {{ incremental_load_filter('_load_timestamp') }}
        -- sample for dev testing TODO remove before prod merge
        -- AND block_id BETWEEN 52000000 AND 54000000
),
FINAL AS (
    SELECT
        block_id,
        shard_id,
        _load_timestamp,
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
    *
FROM
    FINAL
