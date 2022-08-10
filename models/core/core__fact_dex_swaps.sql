{{ config(
    materialized = 'view',
    secure = true
) }}

WITH dex_swaps AS (

    SELECT
        *
    FROM
        {{ ref('silver__dex_swaps') }}
)
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    action_id,
    tx_signer,
    tx_receiver,
    pool_id,
    token_in,
    amount_in,
    token_out,
    amount_out,
    swap_index
FROM
    dex_swaps
