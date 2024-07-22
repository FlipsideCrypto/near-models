{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH token_transfers AS (

    SELECT
        *
    FROM
        {{ ref('silver__token_transfers') }}
),
labels AS (
    SELECT
        contract_address,
        NAME,
        symbol,
        decimals
    FROM
        {{ ref('silver__ft_contract_metadata') }}
)

SELECT
    block_id,
    block_timestamp,
    tx_hash,
    action_id,
    contract_address,
    from_address,
    to_address,
    memo,
    amount_raw,
    amount_raw_precise,
    IFF(
        C.decimals IS NOT NULL,
        utils.udf_decimal_adjust(
            amount_raw_precise,
            C.decimals
        ),
        NULL
    ) AS amount_precise,
    amount_precise :: FLOAT AS amount,
    IFF(
        C.decimals IS NOT NULL
        AND price IS NOT NULL,
        amount * price,
        NULL
    ) AS amount_usd,
    C.decimals AS decimals,
    C.symbol AS symbol,
    price AS token_price,
    CASE
        WHEN C.decimals IS NULL THEN 'false'
        ELSE 'true'
    END AS has_decimal,
    CASE
        WHEN price IS NULL THEN 'false'
        ELSE 'true'
    END AS has_price,
    transfer_type,
    transfers_id AS fact_token_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    token_transfers t
LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
    ON t.contract_address = p.token_address
    AND DATE_TRUNC(
        'hour',
        t.block_timestamp
    ) = HOUR
LEFT JOIN labels C USING (contract_address)