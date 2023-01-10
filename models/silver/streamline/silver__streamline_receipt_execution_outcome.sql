{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'receipt_execution_outcome_id',
    cluster_by = ['_load_timestamp::date','block_id','chunk_hash']
) }}

WITH shards AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_shards') }}
    WHERE
        ARRAY_SIZE(receipt_execution_outcomes) > 0
        AND {{ incremental_load_filter('_load_timestamp') }}
        -- sample for dev testing TODO remove before prod merge
        -- AND block_id BETWEEN 52000000 AND 54000000

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
