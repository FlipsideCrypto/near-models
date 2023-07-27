{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_load_timestamp::DATE'],
    tags = ['curated']
) }}

WITH token_labels AS (

    SELECT
        *
    FROM
        {{ ref('silver__token_labels') }}
),
events_function_call AS (
    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
prices AS (
    SELECT
        block_id,
        tx_hash,
        block_timestamp,
        receiver_id,
        INDEX,
        VALUE :asset_id :: STRING AS token_contract,
        VALUE :price :multiplier :: DOUBLE AS raw_price,
        CASE
            WHEN token_contract IN ('4691937a7508860f876c9c0a2a617e7d9e945d4b.factory.bridge.near') THEN 6
            WHEN token_contract IN (
                'aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near'
            )
            AND block_id > 77644611 THEN 5
            WHEN token_contract IN (
                '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near'
            )
            AND block_id > 77644611 THEN 2
            WHEN token_contract IN (
                '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near'
            )
            AND len(raw_price) < 9 THEN 2
            ELSE 4
        END AS decimals,
        raw_price / pow(
            10,
            decimals
        ) AS price_usd,
        _load_timestamp
    FROM
        events_function_call,
        LATERAL FLATTEN(
            input => args :prices
        )
    WHERE
        method_name = 'report_prices'
),
FINAL AS (
    SELECT
        p.block_id,
        p.tx_hash,
        p.block_timestamp,
        p.index,
        l.token,
        l.symbol,
        p.token_contract,
        p.raw_price,
        p.price_usd,
        p.receiver_id AS source,
        p._load_timestamp
    FROM
        prices p
        LEFT JOIN token_labels l USING (token_contract)
)
SELECT
    *
FROM
    FINAL
WHERE
    token_contract != 'aurora'
