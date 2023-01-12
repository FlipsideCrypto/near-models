{{ config(
    materialized = 'incremental',
    unique_key = 'receipt_object_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['rpc']
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
        block_id,
        tx_hash,
        INDEX AS receipt_index,
        VALUE :id :: STRING AS receipt_object_id,
        VALUE :outcome :receipt_ids :: ARRAY AS receipt_outcome_id,
        VALUE :outcome :executor_id :: STRING AS receiver_id,
        VALUE :outcome :gas_burnt :: NUMBER AS gas_burnt,
        VALUE :outcome :status :: variant AS status_value,
        VALUE :outcome :logs :: ARRAY AS logs,
        VALUE :proof :: ARRAY AS proof,
        VALUE :outcome :metadata :: variant AS metadata,
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
