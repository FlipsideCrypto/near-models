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

{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(DATE_TRUNC('day', block_timestamp)) AS block_timestamp_day
FROM
    {{ ref('defi__fact_intents') }}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_timestamp_day = run_query(query).columns [0].values() [0] %}
    {% elif var('MANUAL_FIX') %}
    {% set query %}
SELECT
    MIN(DATE_TRUNC('day', block_timestamp)) AS block_timestamp_day
FROM
    {{ this }}
WHERE
    FLOOR(
        block_id,
        -3
    ) = {{ var('RANGE_START') }}

    {% endset %}
    {% set min_block_timestamp_day = run_query(query).columns [0].values() [0] %}
{% endif %}

{% if not min_block_timestamp_day or min_block_timestamp_day == 'None' %}
    {% set min_block_timestamp_day = '2024-11-01' %}
{% endif %}

{{ log(
    "min_block_timestamp_day: " ~ min_block_timestamp_day,
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
        SPLIT(
            token_id,
            'nep141:'
        ) [1] AS contract_address_raw,
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
            {{ partition_load_manual('no_buffer', floor(block_id, -3)) }}
        {% else %}

        {% if is_incremental() %}
            WHERE
                GREATEST(
                    modified_timestamp,
                    '2000-01-01'
                ) >= DATEADD(
                    'minute',
                    -5,(
                        SELECT
                            MAX(
                                modified_timestamp
                            )
                        FROM
                            {{ this }}
                    )
                )
        {% endif %}
        {% endif %}
),
labels AS (
    SELECT
        near_token_id AS contract_address_raw,
        SPLIT(
            defuse_asset_identifier,
            ':'
        ) [0] :: STRING AS ecosystem,
        SPLIT(
            defuse_asset_identifier,
            ':'
        ) [1] :: STRING AS chain_id,
        SPLIT(
            defuse_asset_identifier,
            ':'
        ) [2] :: STRING AS contract_address,
        asset_name AS symbol,
        decimals
    FROM
        {{ ref('silver__defuse_tokens_metadata') }}
    UNION ALL
    SELECT
        contract_address AS contract_address_raw,
        'near' AS ecosystem,
        '397' AS chain_id,
        contract_address,
        symbol,
        decimals
    FROM
        {{ ref('silver__ft_contract_metadata') }}
    WHERE
        contract_address not in (
            select distinct near_token_id 
            from {{ ref('silver__defuse_tokens_metadata') }}
        )
),
prices AS (
    SELECT
        token_address AS contract_address,
        symbol,
        is_native,
        price,
        HOUR
    FROM
        {{ ref('price__ez_prices_hourly') }}

{% if is_incremental() or var('MANUAL_FIX') %}
WHERE
    DATE_TRUNC(
        'day',
        HOUR
    ) >= '{{ min_block_timestamp_day }}'
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
        COALESCE(
            dl.short_name,
            l.ecosystem
        ) AS blockchain,
        l.contract_address,
        l.contract_address = 'native' AS is_native,
        l.symbol,
        l.decimals,
        amount_raw / pow(
            10,
            l.decimals
        ) AS amount_adj,
        -- We do not have USDC for every token on every chain, so fallback to 1
        IFF(l.symbol ilike 'USD%', COALESCE(p.price, 1), COALESCE(p.price, p2.price)) AS price,
        amount_raw / pow(
            10,
            l.decimals
        ) * IFF(l.symbol ilike 'USD%', COALESCE(p.price, 1), COALESCE(p.price, p2.price)) AS amount_usd
    FROM
        intents i
        LEFT JOIN labels l
        ON i.contract_address_raw = l.contract_address_raw
        LEFT JOIN EXTERNAL.defillama.dim_chains dl
        ON l.chain_id = dl.chain_id 
        ASOF JOIN prices p match_condition (
            i.block_timestamp >= p.hour
        )
        ON (
            l.contract_address = p.contract_address
        )
        ASOF JOIN prices p2 match_condition (
            i.block_timestamp >= p2.hour
        )
        ON (
            (l.contract_address = 'native') = p2.is_native
            AND upper(l.symbol) = upper(p2.symbol)
        )
)
SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL
