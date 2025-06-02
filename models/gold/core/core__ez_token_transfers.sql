{{ config(
    materialized = 'incremental',
    unique_key = "ez_token_transfers_id",
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'floor(block_id, -3)'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,contract_address,from_address,to_address,ez_token_transfers_id);",    
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% if is_incremental() and not var("MANUAL_FIX") %}
        {% set query %}

        SELECT
            MIN(DATE_TRUNC('day', block_timestamp)) AS block_timestamp_day
        FROM
            {{ ref('core__fact_token_transfers') }}
        WHERE
            modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            ) 
            {% endset %}
            {% set min_block_timestamp_day = run_query(query).columns [0].values() [0] %}
    {% elif var('MANUAL_FIX') %}
            {% set query %}
            SELECT
                MIN(DATE_TRUNC('day', block_timestamp)) AS block_timestamp_day
            FROM
                {{ this }}
            WHERE
                FLOOR(block_id, -3) = {{ var('RANGE_START') }}
            {% endset %}
            {% set min_block_timestamp_day = run_query(query).columns [0].values() [0] %}
    {% endif %}
    {% if not min_block_timestamp_day or min_block_timestamp_day == 'None' %}
        {% set min_block_timestamp_day = '2020-07-01' %}
    {% endif %}
{{ log("min_block_timestamp_day: " ~ min_block_timestamp_day, info=True) }}
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
),
FINAL AS (
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    receipt_id,
    contract_address,
    from_address,
    to_address,
    memo,
    -- TODO do we need so many amount columns?
    amount_unadj AS amount_raw,
    TRY_CAST(amount_unadj AS FLOAT) AS amount_raw_precise,
    IFF(
        C.decimals IS NOT NULL,
        utils.udf_decimal_adjust(
            amount_raw_precise,
            C.decimals
        ),
        NULL
    ) AS amount_precise,
    TRY_CAST(amount_precise AS FLOAT) AS amount,
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
    transfer_action,
    receipt_succeeded,
    fact_token_transfers_id AS ez_token_transfers_id
FROM
    {{ ref('core__fact_token_transfers') }}
    t

    {% if is_incremental() %}
        ASOF JOIN hourly_prices p
        MATCH_CONDITION (t.block_timestamp >= p.HOUR)
        ON (t.contract_address = p.token_address)
    {% else %}
        LEFT JOIN hourly_prices p
        ON (t.contract_address = p.token_address)
        AND date_trunc('hour', t.block_timestamp) = p.HOUR
    {% endif %}
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
)
SELECT
    *,
    receipt_id AS action_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL

qualify(row_number() over (partition by ez_token_transfers_id order by modified_timestamp desc)) = 1
