{{ config(
    materialized = 'incremental',
    unique_key = "ez_token_transfers_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', 'floor(block_id, -3)'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% if is_incremental() and not var('MANUAL_FIX') %}
        {{ log("Incremental load", info=True) }}
        {% set query %}

        SELECT
            MIN(DATE_TRUNC('day', block_timestamp)) AS block_timestamp_day
        FROM
            {{ ref('silver__token_transfers_complete') }}
        WHERE
            modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            ) 
            {% endset %}
            {% set min_block_timestamp_day = run_query(query).columns [0].values() [0] %}
            {{ log("Incremental query executed, min_block_timestamp_day: " ~ min_block_timestamp_day, info=True) }}
    {% elif var('MANUAL_FIX') %}
            {{ log("MANUAL_FIX: " ~ var('MANUAL_FIX'), info=True) }}
            {% set query %}
            SELECT
                MIN(DATE_TRUNC('day', block_timestamp)) AS block_timestamp_day
            FROM
                {{ this }}
            WHERE
                FLOOR(block_id, -3) = {{ var('RANGE_START') }}
            {% endset %}
            {% set min_block_timestamp_day = run_query(query).columns [0].values() [0] %}
            {{ log("Query executed, min_block_timestamp_day: " ~ min_block_timestamp_day, info=True) }}
    {% endif %}

    {% if not min_block_timestamp_day or min_block_timestamp_day == 'None' %}
        {{ log("min_block_timestamp_day not set, setting to 2020-07-01", info=True) }}
        {% set min_block_timestamp_day = '2020-07-01' %}
    {% endif %}

{{ log("Final min_block_timestamp_day: " ~ min_block_timestamp_day, info=True) }}
{% endif %}

    WITH hourly_prices AS (
        SELECT
            token_address,
            price,
            HOUR,
            modified_timestamp
        FROM
            {{ ref('price__ez_prices_hourly') }}

{% if is_incremental() or var('MANUAL_FIX') %}
WHERE
    date_trunc('day', HOUR) >= '{{ min_block_timestamp_day }}'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY token_address, HOUR
ORDER BY
    HOUR DESC) = 1)
)
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    action_id,
    contract_address,
    from_address,
    to_address,
    memo,
    amount_unadj :: STRING AS amount_raw,
    amount_unadj :: FLOAT AS amount_raw_precise,
    IFF(
        C.decimals IS NOT NULL,
        utils.udf_decimal_adjust(
            amount_raw_precise,
            C.decimals
        ),
        NULL
    ) AS amount_precise,
    amount_precise :: FLOAT AS amount,
    IFF(
        C.decimals IS NOT NULL
        AND price IS NOT NULL,
        amount * price,
        NULL
    ) AS amount_usd,
    C.decimals AS decimals,
    C.symbol AS symbol,
    price AS token_price,
    transfer_type,
    transfers_complete_id AS ez_token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__token_transfers_complete') }}
    t
    ASOF JOIN hourly_prices p
    MATCH_CONDITION (t.block_timestamp >= p.HOUR)
    ON (t.contract_address = p.token_address)
    LEFT JOIN {{ ref('silver__ft_contract_metadata') }} C USING (contract_address) 
    {% if var("MANUAL_FIX") %}
    WHERE
        {{ partition_load_manual('no_buffer') }}
    {% else %}

{% if is_incremental() %}
WHERE
    GREATEST(
        t.modified_timestamp,
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
