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
        {{ ref('silver__dex_swaps') }}
    GROUP BY
        1
    HAVING
        swaps = 1
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        swap_id,
        trader,
        platform,
        pool_id,
        token_in_symbol AS token_in,
        token_in AS token_in_contract,
        amount_in,
        token_out_symbol AS token_out,
        token_out AS token_out_contract,
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
    token_in_contract,
    amount_in,
    token_out,
    token_out_contract,
    amount_out
FROM
    FINAL
