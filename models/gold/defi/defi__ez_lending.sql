{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, LENDING' }} },
    tags = ['core']
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
        decimals
    FROM
        {{ ref('silver__ft_contract_metadata') }}
),
prices AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_timestamp,
        token_contract AS contract_address,
        AVG(price_usd) AS price_usd
    FROM
        {{ ref('silver__prices_oracle_s3') }}
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
        l.amount_raw / pow(
            10,
            lb.decimals
        ) AS amount,
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
