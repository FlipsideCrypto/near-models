{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'receipt_execution_outcome_id',
    cluster_by = ['_inserted_timestamp::date'],
    tags = ['load', 'load_shards']
) }}

WITH shards AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_shards') }}
    WHERE
        ARRAY_SIZE(receipt_execution_outcomes) > 0
        AND {{ incremental_load_filter('_inserted_timestamp') }}
),
FINAL AS (
    SELECT
        block_id,
        shard_id,
        INDEX AS receipt_outcome_execution_index,
        concat_ws(
            '-',
            shard_id,
            INDEX
        ) AS receipt_execution_outcome_id,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp,
        chunk :header :chunk_hash :: STRING AS chunk_hash,
        VALUE :execution_outcome :: OBJECT AS execution_outcome,
        VALUE :receipt :: OBJECT AS receipt,
        VALUE :receipt :receipt_id :: STRING AS receipt_id,
        VALUE :execution_outcome :id :: STRING AS receipt_outcome_id
    FROM
        shards,
        LATERAL FLATTEN(
            input => receipt_execution_outcomes
        )
)
SELECT
    *
FROM
    FINAL
