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
    value :id :: string as receipt_object_id,
    value :outcome :receipt_ids as receipt_outcome_id,
    value :outcome :status as status_value,
    value :outcome :logs as logs,
    value :proof as proof,
    value :outcome :metadata as metadata
FROM
    {{ ref('transactions') }},
    LATERAL FLATTEN(
        input => tx_receipt
    )
ORDER BY
    block_timestamp DESC
