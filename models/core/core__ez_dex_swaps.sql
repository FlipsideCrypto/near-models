{{ config(
    materialized = 'view',
    secure = true
) }}

WITH dex_swaps AS (

    SELECT
        *
    FROM
        {{ ref('silver__dex_swaps') }}
),
unique_swap_ids AS (
    SELECT
        swap_id,
        COUNT(1) AS swaps
    FROM
        near_dev.silver.dex_swaps
    GROUP BY
        1
    HAVING
        swaps = 1
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
    amount_in,
    token_out,
    amount_out,
    swap_index
FROM
    dex_swaps
WHERE
    swap_id IN (
        SELECT
            swap_id
        FROM
            unique_swap_ids
    )
    AND amount_in IS NOT NULL
    AND amount_out IS NOT NULL
