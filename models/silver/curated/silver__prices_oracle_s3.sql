{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_load_timestamp::DATE'],
    tags = ['curated']
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_transactions_final') }}

        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
token_labels AS (
    SELECT
        *
    FROM
        {{ ref('silver__token_labels') }}
),
oracle_msgs AS (
    SELECT
        block_id,
        tx_hash,
        block_timestamp,
        tx_receiver,
        ARRAY_SIZE(
            tx :actions
        ) AS actions_len,
        tx :actions [0] :FunctionCall :method_name :: STRING AS method_name,
        TRY_PARSE_JSON(
            TRY_BASE64_DECODE_STRING(
                tx :actions [0] :FunctionCall :args
            )
        ) AS response,
        tx,
        _load_timestamp
    FROM
        txs
    WHERE
        tx_receiver = 'priceoracle.near'
        AND method_name = 'report_prices'
),
prices AS (
    SELECT
        block_id,
        tx_hash,
        block_timestamp,
        tx_receiver,
        actions_len,
        INDEX,
        VALUE :asset_id :: STRING AS token_contract,
        VALUE :price :multiplier :: DOUBLE AS raw_price,
        CASE
            WHEN token_contract IN ('4691937a7508860f876c9c0a2a617e7d9e945d4b.factory.bridge.near') THEN 6
            WHEN token_contract IN (
                'aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near'
            ) THEN 5
            WHEN token_contract IN (
                '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near'
            ) and block_id > 77644611 THEN 2
            WHEN token_contract IN (
                '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near'
            ) and len(raw_price) < 9 THEN 2
            ELSE 4
        END AS decimals,
        raw_price / pow(
            10,
            decimals
        ) AS price_usd,
        _load_timestamp
    FROM
        oracle_msgs,
        LATERAL FLATTEN(
            input => response :prices
        )
),
add_labels AS (
    SELECT
        p.block_id,
        p.tx_hash,
        p.block_timestamp,
        p.actions_len,
        p.index,
        l.token,
        l.symbol,
        p.token_contract,
        p.raw_price,
        p.price_usd,
        p.tx_receiver AS source,
        p._load_timestamp
    FROM
        prices p
        LEFT JOIN token_labels l USING (token_contract)
)
SELECT
    *
FROM
    add_labels
WHERE
    token_contract != 'aurora'
