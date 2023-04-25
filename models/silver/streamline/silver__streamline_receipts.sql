{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'receipt_id',
    cluster_by = ['_load_timestamp::date', 'block_id'],
    tags = ['load', 'load_shards']
) }}

WITH receipt_execution_outcomes AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipt_execution_outcome') }}
    WHERE
        {{ incremental_load_filter('_load_timestamp') }}
),
FINAL AS (
    SELECT
        receipt :receipt_id :: STRING AS receipt_id,
        block_id,
        shard_id,
        receipt_outcome_execution_index AS receipt_index,
        chunk_hash,
        receipt,
        execution_outcome,
        execution_outcome :outcome :status :Failure IS NULL AS receipt_succeeded,
        object_keys(
            execution_outcome :outcome :status :Failure
        ) [0] :: STRING AS error_type_0,
        object_keys(
            execution_outcome :outcome :status :Failure [error_type_0] :kind
        ) [0] :: STRING AS error_type_1,
        object_keys(
            execution_outcome :outcome :status :Failure [error_type_0] :kind [error_type_1]
        ) [0] :: STRING AS error_type_2,
        execution_outcome :outcome :status :Failure [error_type_0] :kind [error_type_1] [error_type_2] :: STRING AS error_message,
        execution_outcome :outcome :receipt_ids :: ARRAY AS outcome_receipts,
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
        receipt_execution_outcomes
)
SELECT
    *
FROM
    FINAL
