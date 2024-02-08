{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

{# TODO NOTE 12/11/2023 - d+i on block_id to update timestamp when it comes in. Can be improved. Block id is not properly unique. #}

WITH token_labels AS (

    SELECT
        *
    FROM
        {{ ref('silver__token_labels') }}
),
events_function_call AS (
    SELECT
        block_id,
        tx_hash,
        block_timestamp,
        receiver_id,
        method_name,
        args,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if var('IS_MIGRATION') %}
                WHERE {{ incremental_load_filter('_inserted_timestamp') }}
            {% else %}
                WHERE {{ incremental_load_filter('_modified_timestamp') }}
            {% endif %}
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
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
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
        p._partition_by_block_number,
        p._inserted_timestamp,
        p._modified_timestamp
    FROM
        prices p
        LEFT JOIN token_labels l USING (token_contract)
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'token_contract']
    ) }} AS prices_oracle_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    token_contract != 'aurora'
