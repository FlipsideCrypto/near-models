{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['fees_dip4_id'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}
-- depends on {{ ref('silver__logs_dip4') }}
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
                MIN(DATE_TRUNC('day', block_timestamp)) - INTERVAL '1 day' AS block_timestamp_day
            FROM
                {{ ref('silver__logs_dip4') }}
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

WITH base_fees AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        log_index,
        log_event_index,
        account_id,
        intent_hash,
        referral,
        gas_burnt,
        receipt_succeeded,
        dip4_id,
        raw_log_json :data[0]:fees_collected :: OBJECT AS fees_collected_object,
        modified_timestamp
    FROM
        {{ ref('silver__logs_dip4') }}
    WHERE
        raw_log_json :data[0]:fees_collected IS NOT NULL

    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
flatten_fees AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        log_index,
        log_event_index,
        account_id,
        intent_hash,
        referral,
        gas_burnt,
        receipt_succeeded,
        dip4_id,
        KEY :: STRING AS fee_token_id,
        VALUE :: STRING AS fee_amount_raw,
        REGEXP_SUBSTR(
            KEY :: STRING,
            'nep(141|171|245):(.*)',
            1,
            1,
            'e',
            2
        ) AS fee_asset_identifier
    FROM
        base_fees,
        LATERAL FLATTEN(
            input => fees_collected_object
        )
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
        is_verified,
        HOUR
    FROM
        {{ source('crosschain_price', 'ez_prices_hourly') }}
    WHERE
        NOT is_native

{% if is_incremental() or var('MANUAL_FIX') %}
    AND DATE_TRUNC(
            'day',
            HOUR
        ) >= '{{ min_bd }}'
{% endif %}

    qualify(ROW_NUMBER() over (PARTITION BY COALESCE(token_address, symbol), HOUR
    ORDER BY
        HOUR DESC, IS_VERIFIED DESC) = 1)
),
prices_native AS (
    SELECT
        token_address AS contract_address,
        symbol,
        price,
        is_native,
        is_verified,
        HOUR
    FROM
        {{ source('crosschain_price', 'ez_prices_hourly') }}
    WHERE
        is_native

{% if is_incremental() or var('MANUAL_FIX') %}
    AND DATE_TRUNC(
            'day',
            HOUR
        ) >= '{{ min_bd }}'
{% endif %}

    qualify(ROW_NUMBER() over (PARTITION BY COALESCE(token_address, symbol), HOUR
    ORDER BY
        HOUR DESC, IS_VERIFIED DESC) = 1)
)
SELECT
    f.block_timestamp,
    f.block_id,
    f.tx_hash,
    f.receipt_id,
    f.receiver_id,
    f.predecessor_id,
    f.log_event,
    f.dip4_version,
    f.log_index,
    f.log_event_index,
    f.account_id,
    f.intent_hash,
    f.referral,
    f.gas_burnt,
    f.receipt_succeeded,
    f.dip4_id,
    f.fee_token_id,
    f.fee_asset_identifier,
    f.fee_amount_raw,
    l.source_chain AS fee_blockchain,
    l.crosschain_token_contract AS fee_contract_address,
    l.crosschain_token_contract = 'native' AS fee_is_native,
    l.symbol AS fee_symbol,
    l.decimals AS fee_decimals,
    f.fee_amount_raw :: NUMBER / pow(
        10,
        l.decimals
    ) AS fee_amount_adj,
    IFF(l.symbol ilike 'USD%', COALESCE(p.price, 1), COALESCE(p.price, p2.price)) AS fee_price,
    ZEROIFNULL(
        f.fee_amount_raw :: NUMBER / pow(10, l.decimals) * IFF(
            l.symbol ilike 'USD%',
            COALESCE(p.price, 1),
            COALESCE(p.price, p2.price)
        )
    ) AS fee_amount_usd,
    COALESCE(p.is_verified, p2.is_verified, FALSE) AS fee_token_is_verified,
    {{ dbt_utils.generate_surrogate_key(
        ['f.tx_hash', 'f.receipt_id', 'f.log_index', 'f.log_event_index', 'f.fee_token_id']
    ) }} AS fees_dip4_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_fees f
    LEFT JOIN labels l
    ON f.fee_asset_identifier = l.asset_identifier
    -- price the fee token
    ASOF JOIN prices p match_condition (
        f.block_timestamp >= p.hour
    )
    ON (
        l.crosschain_token_contract = p.contract_address
    )
    ASOF JOIN prices_native p2 match_condition (
        f.block_timestamp >= p2.hour
    )
    ON (
        upper(l.symbol) = upper(p2.symbol)
        AND (l.crosschain_token_contract = 'native') = p2.is_native
    )

qualify(ROW_NUMBER() over (PARTITION BY fees_dip4_id
ORDER BY
    fee_price IS NOT NULL DESC, fee_is_native DESC) = 1)
