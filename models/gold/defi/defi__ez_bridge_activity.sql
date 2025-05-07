{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'ez_bridge_activity_id',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,token_address,destination_address,source_address,platform,bridge_address,destination_chain,source_chain,method_name,direction,receipt_succeeded);",
    tags = ['scheduled_non_core'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, BRIDGING' }} }
) }}
-- depends on {{ ref('defi__fact_bridge_activity') }}
-- depends on {{ ref('silver__ft_contract_metadata') }}
-- depends on {{ ref('silver__complete_token_prices') }}

{% if execute %}

    {% if is_incremental() %}
        {% set max_mod_query %}
            SELECT
                MAX(modified_timestamp) modified_timestamp
            FROM
                {{ this }}
            WHERE
                modified_timestamp >= {{ max_mod }}
        {% endset %}
        {% set max_mod = run_query(max_mod_query) [0] [0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}

        {% set query %}
            SELECT
                MIN(DATE_TRUNC('day', block_timestamp)) AS block_timestamp_day
            FROM
                {{ ref('fact_bridge_activity') }}
            WHERE
                modified_timestamp >= {{ max_mod }}
        {% endset %}
        {% set min_bd = run_query(query).columns [0].values() [0] %}
    {% endif %}

    {% if not min_bd or min_bd == 'None' %}
        {% set min_bd = '2024-11-01' %}
    {% endif %}

    {{ log(
        "min_bd: " ~ min_bd,
        info = True
    ) }}
{% endif %}

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
    {% if var('MANUAL_FIX') %}
        WHERE {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            WHERE modified_timestamp > (
                SELECT MAX(modified_timestamp) FROM {{ this }}
            )
        {% endif %}
    {% endif %}
),
labels AS (
    SELECT 
        contract_address,
        name,
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
    {% if is_incremental() or var('MANUAL_FIX') %}

        WHERE
            DATE_TRUNC(
                'day',
                HOUR
            ) >= '{{ min_bd }}'
    {% endif %}
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
            WHEN contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'aurora'
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
        COALESCE(w.near_contract_address, b.token_address) AS token_address,
        b.amount_unadj,
        b.amount_adj,
        COALESCE(w.symbol, l1.symbol) as symbol,
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
    FROM fact_bridging b
        LEFT JOIN {{ ref('seeds__portalbridge_tokenids') }} w
            ON b.token_address = w.wormhole_contract_address
        LEFT JOIN labels l1
            ON COALESCE(w.near_contract_address, b.token_address) = l1.contract_address
        LEFT JOIN prices_mapping p1
            ON COALESCE(w.near_contract_address, b.token_address) = p1.contract_address
            AND DATE_TRUNC('hour', b.block_timestamp) = p1.block_timestamp
)
SELECT
    *
FROM
    FINAL
