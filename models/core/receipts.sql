{{ config(
    materialized = 'incremental',
    unique_key = 'txn_hash',
    incremental_strategy = 'delete+insert',
    tags = ['core', 'transactions'],
    cluster_by = ['block_timestamp']
) }}

SELECT
    block_timestamp,
    block_hash,
    txn_hash,
    VALUE :id :: STRING AS receipt_object_id,
    CASE
        WHEN VALUE :outcome :receipt_ids [1] IS NOT NULL THEN VALUE :outcome :receipt_ids
        ELSE VALUE :outcome :receipt_ids [0] :: STRING
    END AS receipt_outcome_id,
    VALUE :outcome :status AS status_value,
    VALUE :outcome :logs AS logs,
    VALUE :proof AS proof,
    VALUE :outcome :metadata AS metadata
FROM
    {{ ref('transactions') }},
    LATERAL FLATTEN(
        input => tx_receipt
    )
ORDER BY
    block_timestamp DESC
