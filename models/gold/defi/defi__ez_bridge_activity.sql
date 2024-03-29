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
        b.block_id,
        b.block_timestamp,
        b.tx_hash,
        b.token_address,
        b.amount_unadj,
        l1.symbol,
        b.amount_unadj / pow(
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
        LEFT JOIN prices p1
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
