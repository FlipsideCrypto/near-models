{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'receipt_id',
    cluster_by = ['_load_timestamp::date', 'block_id', 'receipt_id']
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
        chunk_hash,
        VALUE AS receipt
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
        chunk_hash,
        receipt
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
        block_id,
        shard_id,
        source_object,
        object_receipt_index AS receipt_index,
        _load_timestamp,
        chunk_hash,
        receipt,
        receipt :receipt_id :: STRING AS receipt_id,
        receipt :receiver_id :: STRING AS receiver_id,
        receipt :receipt :Action :signer_id :: STRING AS signer_id,
        object_keys(
            receipt :receipt
        ) [0] :: STRING AS receipt_type
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
