{{ config(
    materialized = 'view',
    secure = true,
    tags = ['core']
) }}

WITH transactions AS (

    SELECT
        *
    FROM
        {{ ref('silver__usn_supply_s3') }}
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
