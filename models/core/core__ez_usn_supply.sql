{{ config(
    materialized = 'view',
    secure = true
) }}

WITH transactions AS (

    SELECT
        *
    FROM
        {{ ref('silver__usn_supply') }}
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    status,
    event,
    from_address,
    to_address,
    amount
FROM
    transactions
