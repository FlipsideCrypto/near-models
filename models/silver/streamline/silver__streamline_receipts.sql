{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'receipt_id',
    cluster_by = ['_load_timestamp::date', 'block_id']
) }}

WITH chunks AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_chunks') }}
    WHERE
        ARRAY_SIZE(receipts) > 0
        AND {{ incremental_load_filter('_load_timestamp') }}
),
receipt_execution_outcomes AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipt_execution_outcome') }}
    WHERE
        {{ incremental_load_filter('_load_timestamp') }}
),
chunk_receipts AS (
    SELECT
        block_id,
        shard_id,
        'chunk' AS source_object,
        INDEX AS object_receipt_index,
        _load_timestamp,
        _partition_by_block_number,
        chunk_hash,
        VALUE AS receipt,{} AS execution_outcome,
        [] AS outcome_receipts
    FROM
        chunks,
        LATERAL FLATTEN(
            input => receipts
        )
    WHERE
        ARRAY_SIZE(receipts) > 0
),
reo_receipts AS (
    SELECT
        block_id,
        shard_id,
        'receipt_execution_outcomes' AS source_object,
        receipt_outcome_execution_index AS object_receipt_index,
        _load_timestamp,
        _partition_by_block_number,
        chunk_hash,
        receipt,
        execution_outcome,
        execution_outcome :outcome :receipt_ids :: ARRAY AS outcome_receipts
    FROM
        receipt_execution_outcomes
),
receipts AS (
    SELECT
        *
    FROM
        chunk_receipts
    UNION
    SELECT
        *
    FROM
        reo_receipts
),
FINAL AS (
    SELECT
        receipt :receipt_id :: STRING AS receipt_id,
        block_id,
        shard_id,
        source_object,
        object_receipt_index AS receipt_index,
        chunk_hash,
        receipt,
        execution_outcome,
        outcome_receipts,
        receipt :receiver_id :: STRING AS receiver_id,
        receipt :receipt :Action :signer_id :: STRING AS signer_id,
        LOWER(
            object_keys(
                receipt :receipt
            ) [0] :: STRING
        ) AS receipt_type,
        _load_timestamp,
        _partition_by_block_number
    FROM
        receipts qualify ROW_NUMBER() over (
            PARTITION BY receipt_id
            ORDER BY
                source_object DESC,
                _load_timestamp DESC
        ) = 1
)
SELECT
    *
FROM
    FINAL
