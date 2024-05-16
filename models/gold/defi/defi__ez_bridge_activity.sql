{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} },
    tags = ['core']
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
                hour
            ) AS block_timestamp,
        token_address AS contract_address,
        AVG(price) AS price_usd,
        MAX(SYMBOL) AS symbol
    FROM
        {{ ref('silver__complete_token_prices') }}
    GROUP BY
        1,
        2
),
prices_mapping AS (
    SELECT
        block_timestamp,
        CASE
            WHEN contract_address = '0xf7413489c474ca4399eee604716c72879eea3615' THEN 'apys.token.a11bd.near'
            WHEN contract_address = '0x3294395e62f4eb6af3f1fcf89f5602d90fb3ef69' THEN 'celo.token.a11bd.near'
            WHEN contract_address = '0xd2877702675e6ceb975b4a1dff9fb7baf4c91ea9' THEN 'luna.token.a11bd.near'
            WHEN contract_address = '0xa47c8bf37f92abed4a126bda807a7b7498661acd' THEN 'ust.token.a11bd.near'
            WHEN contract_address = 'aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near' THEN 'aurora'
            ELSE contract_address
        END AS contract_address,
        symbol,
        price_usd
    FROM
        prices
),
FINAL AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        b.tx_hash,
        b.token_address,
        b.amount_unadj,
        l1.symbol,
        b.amount_adj / pow(
            10,
            l1.decimals
        ) AS amount,
        amount * p1.price_usd AS amount_usd,
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
    FROM
        fact_bridging b
        LEFT JOIN labels l1
        ON b.token_address = l1.contract_address
        LEFT JOIN prices_mapping p1
        ON b.token_address = p1.contract_address
        AND DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p1.block_timestamp
)
SELECT
    *
FROM
    FINAL
