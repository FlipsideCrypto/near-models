{{ config(
    materialized = 'incremental',
    unique_key = 'receipt_object_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
),
receipts AS (
    SELECT
        block_timestamp,
        block_hash,
        tx_hash,
        VALUE :id :: STRING AS receipt_object_id,
        VALUE :outcome :receipt_ids AS receipt_outcome_id,
        VALUE :outcome :status AS status_value,
        VALUE :outcome :logs AS logs,
        VALUE :proof AS proof,
        VALUE :outcome :metadata AS metadata,
        _ingested_at,
        _inserted_timestamp
    FROM
        txs,
        LATERAL FLATTEN(
            input => tx: receipt
        )
)
SELECT
    *
FROM
    receipts
