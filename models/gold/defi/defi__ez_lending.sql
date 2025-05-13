{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'ez_lending_id',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,contract_address,token_address,sender_id,actions,amount_raw,amount_adj);",
    tags = ['scheduled_non_core'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, LENDING' }} }
) }}
-- depends on {{ ref('silver__burrow_lending') }}
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
                {{ ref('silver__burrow_lending') }}
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
    {% if var('MANUAL_FIX') %}
        WHERE {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            WHERE modified_timestamp > {{ max_mod }}
        {% endif %}
    {% endif %}
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
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
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
