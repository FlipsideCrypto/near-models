{{ config(
    materialized = 'incremental',
    unique_key = 'receipt_object_id',
    incremental_strategy = 'delete+insert',
    tags = ['core', 'transactions'],
    cluster_by = ['ingested_at::DATE', 'block_timestamp::DATE'],
    enabled = false
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('transactions') }}
    WHERE
        {{ incremental_last_x_days(
            "ingested_at",
            2
        ) }}
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
        VALUE :outcome :metadata AS metadata,
        ingested_at
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
