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
    block_timestamp,
    block_id,
    receiver_id,
    signer_id,
    clean_log,
    receipt_object_id,
    gas_burnt
FROM
    logs
