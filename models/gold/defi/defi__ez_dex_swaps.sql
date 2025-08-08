{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, SWAPS' }} },
    tags = ['scheduled_non_core']
) }}

WITH dex_swaps AS (

    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        swap_index,
        amount_out_raw,
        token_out,
        amount_in_raw,
        token_in,
        swap_input_data,
        LOG,
        dex_swaps_v2_id AS ez_dex_swaps_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__dex_swaps_v2') }}
),

intents_swaps AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        swap_index,
        amount_out_raw,
        token_out,
        amount_in_raw,
        token_in,
        swap_input_data,
        log AS LOG,
        intents_swap_id AS ez_dex_swaps_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__swap_intents') }}
),

all_swaps AS (
    SELECT * FROM dex_swaps
    UNION ALL
    SELECT * FROM intents_swaps
),
labels AS (
    SELECT
        asset_identifier AS contract_address,
        NAME,
        symbol,
        decimals
    FROM
        {{ ref('silver__ft_contract_metadata') }}
),
prices AS (
    SELECT
        DATE_TRUNC(
            'hour',
            hour
        ) AS block_timestamp,
        token_address AS contract_address,
        AVG(price) AS price_usd,
        MAX(is_verified) AS token_is_verified
    FROM
        {{ ref('silver__complete_token_prices') }}
    GROUP BY
        1,
        2
),
FINAL AS (
    SELECT
        s.tx_hash,
        s.receipt_id,
        s.block_id,
        s.block_timestamp,
        s.receiver_id AS platform,
        s.swap_input_data :pool_id :: INT AS pool_id,
        s.signer_id AS trader,
        s.swap_index,
        s.amount_out_raw,
        s.amount_out_raw / pow(
            10,
            l1.decimals
        ) AS amount_out,
        amount_out * p1.price_usd AS amount_out_usd,
        s.token_out AS token_out_contract,
        l1.symbol AS symbol_out,
        p1.token_is_verified AS token_out_is_verified,
        s.amount_in_raw,
        s.amount_in_raw / pow(
            10,
            l2.decimals
        ) AS amount_in,
        amount_in * p2.price_usd AS amount_in_usd,
        s.token_in AS token_in_contract,
        l2.symbol AS symbol_in,
        p2.token_is_verified AS token_in_is_verified,
        s.swap_input_data,
        s.log,
        s.ez_dex_swaps_id,
        s.inserted_timestamp,
        s.modified_timestamp
    FROM
        all_swaps s
        LEFT JOIN labels l1
        ON s.token_out = l1.contract_address
        LEFT JOIN labels l2
        ON s.token_in = l2.contract_address
        LEFT JOIN prices p1
        ON DATE_TRUNC(
            'hour',
            s.block_timestamp
        ) = p1.block_timestamp
        AND s.token_out = p1.contract_address
        LEFT JOIN prices p2
        ON DATE_TRUNC(
            'hour',
            s.block_timestamp
        ) = p2.block_timestamp
        AND s.token_in = p2.contract_address
)
SELECT
    *,
    COALESCE(token_in_is_verified, FALSE) AS token_in_is_verified,
    COALESCE(token_out_is_verified, FALSE) AS token_out_is_verified
FROM
    FINAL
