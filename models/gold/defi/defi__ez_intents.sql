{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    unique_key = ['ez_intents_id'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id);",
    tags = ['scheduled_non_core']
) }}
-- depends on {{ ref('defi__fact_intents') }}
-- depends on {{ ref('silver__ft_contract_metadata') }}
-- depends on {{ ref('price__ez_prices_hourly') }}

{% if execute %}

    {% if is_incremental() %}
        {% set max_mod_query %}
            SELECT
                MAX(modified_timestamp) modified_timestamp
            FROM
                {{ this }}
        {% endset %}
        {% set max_mod = run_query(max_mod_query) [0] [0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}

        
    {% set query %}

        SELECT
            MIN(DATE_TRUNC('day', block_timestamp))  - INTERVAL '1 day' AS block_timestamp_day
        FROM
            {{ ref('defi__fact_intents') }}
        WHERE
            modified_timestamp >= '{{ max_mod }}'
    {% endset %}
    
        {% set min_bd = run_query(query).columns [0].values() [0] %}
    {% elif var('MANUAL_FIX') %}
        {% set query %}
            SELECT
                MIN(DATE_TRUNC('day', block_timestamp)) - INTERVAL '1 day' AS block_timestamp_day
            FROM
                {{ this }}
            WHERE
                FLOOR(
                    block_id,
                    -3
                ) = {{ var('RANGE_START') }}

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

WITH intents AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        log_index,
        log_event_index,
        owner_id,
        old_owner_id,
        new_owner_id,
        memo,
        amount_index,
        amount_raw,
        token_id,
        REGEXP_SUBSTR(
            token_id,
            'nep(141|171|245):(.*)',
            1,
            1,
            'e',
            2
        ) AS asset_identifier,
        referral,
        dip4_version,
        gas_burnt,
        receipt_succeeded,
        fact_intents_id,
        FLOOR(
            block_id,
            -3
        ) AS _partition_by_block_number
    FROM
        {{ ref('defi__fact_intents') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer', 'FLOOR(block_id, -3)') }}
        {% else %}

            {% if is_incremental() %}
                WHERE
                    GREATEST(
                        modified_timestamp,
                        '2000-01-01'
                    ) >= DATEADD(
                        'minute',
                        -5,
                        '{{ max_mod }}'
                    )
            {% endif %}
        {% endif %}
),
labels AS (
    SELECT
        asset_identifier,
        source_chain,
        crosschain_token_contract,
        near_token_contract,
        symbol,
        decimals
    FROM
        {{ ref('silver__ft_contract_metadata') }}
),
prices AS (
    SELECT
        token_address AS contract_address,
        blockchain,
        symbol,
        price,
        is_native,
        HOUR
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        NOT is_native

{% if is_incremental() or var('MANUAL_FIX') %}
AND
    DATE_TRUNC(
        'day',
        HOUR
    ) >= '{{ min_bd }}'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY COALESCE(token_address, symbol), HOUR
ORDER BY
    HOUR DESC) = 1)
),
prices_native AS (
    SELECT
        token_address AS contract_address,
        symbol,
        price,
        is_native,
        HOUR
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        is_native

{% if is_incremental() or var('MANUAL_FIX') %}
AND
    DATE_TRUNC(
        'day',
        HOUR
    ) >= '{{ min_bd }}'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY COALESCE(token_address, symbol), HOUR
ORDER BY
    HOUR DESC) = 1)
),
FINAL AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        log_index,
        log_event_index,
        owner_id,
        old_owner_id,
        new_owner_id,
        memo,
        amount_index,
        amount_raw,
        token_id,
        referral,
        dip4_version,
        gas_burnt,
        receipt_succeeded,
        fact_intents_id AS ez_intents_id,
        l.source_chain AS blockchain,
        l.crosschain_token_contract AS contract_address,
        l.crosschain_token_contract = 'native' AS is_native,
        l.symbol,
        l.decimals,
        amount_raw / pow(
            10,
            l.decimals
        ) AS amount_adj,
        -- We do not have USDC for every token on every chain, so fallback to 1
        IFF(l.symbol ilike 'USD%', COALESCE(p.price, 1), COALESCE(p.price, p2.price)) AS price,
        ZEROIFNULL(
            amount_raw / pow(10, l.decimals) * IFF(
                l.symbol ilike 'USD%',
                COALESCE(p.price, 1),
                COALESCE(p.price, p2.price)
            )
        ) AS amount_usd
    FROM
        intents i
        LEFT JOIN labels l
        ON i.token_id = l.asset_identifier
        ASOF JOIN prices p match_condition (
            i.block_timestamp >= p.hour
        )
        ON (
            l.crosschain_token_contract = p.contract_address
        )
        ASOF JOIN prices_native p2 match_condition (
            i.block_timestamp >= p2.hour
        )
        ON (
            upper(l.symbol) = upper(p2.symbol)
            AND (l.crosschain_token_contract = 'native') = p2.is_native
        )
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    token_id,
    symbol,
    amount_adj,
    amount_usd,
    owner_id,
    old_owner_id,
    new_owner_id,
    amount_raw,
    blockchain,
    contract_address,
    is_native,
    price,
    decimals,
    gas_burnt,
    memo,
    referral,
    dip4_version,
    log_index,
    log_event_index,
    amount_index,
    receipt_succeeded,
    ez_intents_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL

qualify(ROW_NUMBER() over (PARTITION BY ez_intents_id
ORDER BY
    price IS NOT NULL DESC, is_native DESC) = 1)
