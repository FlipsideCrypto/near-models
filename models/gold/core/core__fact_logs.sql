{{ config(
    materialized = 'view',
    secure = true,
    tags = ['core']
) }}

WITH logs AS (

    SELECT
        *
    FROM
        {{ ref('silver__logs_s3') }}
)
SELECT
    tx_hash,
    receiver_id,
    signer_id,
    clean_log,
    receipt_object_id,
    block_id,
    gas_burnt,
    block_timestamp,
    tx_hash,
    block_id,
    block_timestamp
FROM
    logs
