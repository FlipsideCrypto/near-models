{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, LENDING' }} },
    tags = ['scheduled_non_core']
) }}

WITH lending AS (

    SELECT
        platform,
        tx_hash,
        block_id,
        block_timestamp,
        sender_id,
        actions,
        contract_address,
        amount_raw,
        amount_adj,
        burrow_lending_id AS ez_lending_id,
        token_address,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__burrow_lending') }}
),
labels AS (
    SELECT
        contract_address,
        NAME,
        symbol,
        IFF(
            contract_address IN (
                -- wbtc
                '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near',
                -- usdc.e
                'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
                -- usdc
                '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1',
                -- usdt
                'usdt.tether-token.near',
                -- usdt.e
                'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            ),
            18,
            decimals
        ) AS decimals
    FROM
        {{ ref('silver__ft_contract_metadata') }}
),
prices AS (
    SELECT
        DATE_TRUNC(
            'hour',
            HOUR
        ) AS block_timestamp,
        token_address AS contract_address,
        AVG(price) AS price_usd
    FROM
        {{ ref('silver__complete_token_prices') }}
    GROUP BY
        1,
        2
),
FINAL AS (
    SELECT
        l.platform,
        l.tx_hash,
        l.block_id,
        l.block_timestamp,
        l.sender_id,
        l.actions,
        l.contract_address,
        l.token_address,
        lb.name,
        lb.symbol,
        l.amount_raw,
        l.amount_adj,
        l.amount_adj / pow(
            10,
            lb.decimals
        ) AS amount,
        p.price_usd,
        amount * p.price_usd AS amount_usd,
        l.ez_lending_id,
        l.inserted_timestamp,
        l.modified_timestamp
    FROM
        lending l
        LEFT JOIN labels lb
        ON l.token_address = lb.contract_address
        LEFT JOIN prices p
        ON DATE_TRUNC(
            'hour',
            l.block_timestamp
        ) = p.block_timestamp
        AND l.token_address = p.contract_address
)
SELECT
    *
FROM
    FINAL
