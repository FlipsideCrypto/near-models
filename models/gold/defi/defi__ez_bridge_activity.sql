{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} },
    tags = ['scheduled_non_core']
) }}

WITH fact_bridging AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_unadj,
        amount_adj,
        destination_address,
        source_address,
        platform,
        bridge_address,
        destination_chain,
        source_chain,
        method_name,
        direction,
        receipt_succeeded,
        fact_bridge_activity_id AS ez_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('defi__fact_bridge_activity') }}
),
labels AS (
    SELECT 
        asset_identifier AS contract_address,
        name,
        source_chain,
        crosschain_token_contract,
        near_token_contract,
        symbol,
        decimals
    FROM 
        {{ ref('silver__ft_contract_metadata') }}
    WHERE crosschain_token_contract IS NOT NULL
    QUALIFY(ROW_NUMBER() OVER (
        PARTITION BY asset_identifier
        ORDER BY asset_identifier
    ) = 1)
),
prices AS (
    SELECT DISTINCT
        token_address,
        blockchain,
        symbol,
        price,
        decimals,
        is_native,
        is_verified,
        hour
    FROM
        {{ ref('silver__complete_token_prices') }}
    WHERE
        NOT is_native
        AND is_verified
    QUALIFY(ROW_NUMBER() OVER (
        PARTITION BY token_address, DATE_TRUNC('hour', hour)
        ORDER BY hour DESC
    ) = 1)
),
prices_native AS (
    SELECT DISTINCT
        'native' AS token_address,
        symbol,
        price,
        decimals,
        is_native,
        is_verified,
        hour
    FROM
        {{ ref('silver__complete_native_prices') }}
    WHERE
        is_native
        AND is_verified
    QUALIFY(ROW_NUMBER() OVER (
        PARTITION BY name, DATE_TRUNC('hour', hour)
        ORDER BY hour DESC
    ) = 1)
),
FINAL AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        b.tx_hash,
        COALESCE(w.near_contract_address, b.token_address) AS token_address,
        b.amount_unadj,
        COALESCE(w.symbol, l1.symbol, p1.symbol, p2.symbol) as symbol,
        b.amount_adj / pow(
            10,
            COALESCE(l1.decimals, p1.decimals, p2.decimals)
        ) AS amount,
        COALESCE(p1.price, p2.price) AS price,
        amount * COALESCE(p1.price, p2.price) AS amount_usd,
        COALESCE(p1.is_verified, p2.is_verified, FALSE) AS token_is_verified,
        b.destination_address,
        b.source_address,
        b.platform,
        b.bridge_address,
        b.destination_chain,
        b.source_chain,
        b.method_name,
        b.direction,
        b.receipt_succeeded,
        b.ez_bridge_activity_id,
        b.inserted_timestamp,
        b.modified_timestamp
    FROM fact_bridging b
        LEFT JOIN {{ ref('seeds__portalbridge_tokenids') }} w
            ON b.token_address = w.wormhole_contract_address
        LEFT JOIN labels l1 ON (
            COALESCE(w.near_contract_address, b.token_address) = l1.contract_address
        )
        LEFT JOIN prices_crosschain p1 ON (
            COALESCE(l1.crosschain_token_contract, b.token_address) = p1.token_address
            AND DATE_TRUNC('hour', b.block_timestamp) = DATE_TRUNC('hour', p1.hour)
        )
        LEFT JOIN prices_native p2 ON (
            UPPER(l1.symbol) = UPPER(p2.symbol)
            AND l1.crosschain_token_contract = p2.token_address
            AND DATE_TRUNC('hour', b.block_timestamp) = DATE_TRUNC('hour', p2.hour)
        )
)
SELECT
    *
FROM
    FINAL
