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
    swap_id,
    trader,
    platform,
    pool_id,
    token_in,
    amount_in / 10e18 AS amount_in,
    token_out,
    amount_out / 10e18 AS amount_out,
    swap_index
FROM
    dex_swaps
