{{ config(
    materialized = 'view',
    secure = true
) }}

WITH transactions AS (

    SELECT
        *
    FROM
        {{ ref('silver__usn') }}
)
SELECT
    block_timestamp,
    block_id,
    method_names,
    tx_hash,
    tx_receiver,
    tx_signer,
    amount
FROM
    transactions
