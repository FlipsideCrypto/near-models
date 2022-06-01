{{ config(
    materialized = 'incremental',
    unique_key = 'receipt_object_id',
    incremental_strategy = 'delete+insert',
    tags = ['core', 'transactions'],
    cluster_by = ['block_timestamp']
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('transactions') }}
    WHERE
        {{ incremental_load_filter('ingested_at') }}
),
receipts AS (
    SELECT
        block_timestamp,
        block_hash,
        txn_hash,
        VALUE :id :: STRING AS receipt_object_id,
        VALUE :outcome :receipt_ids AS receipt_outcome_id,
        VALUE :outcome :status AS status_value,
        VALUE :outcome :logs AS logs,
        VALUE :proof AS proof,
        VALUE :outcome :metadata AS metadata
    FROM
        txs,
        LATERAL FLATTEN(
            input => tx_receipt
        )
    ORDER BY
        block_timestamp DESC
)
SELECT
    *
FROM
    receipts
