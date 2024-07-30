{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    unique_key = "fact_token_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core']
) }}

WITH token_transfers AS (

    SELECT
        *
    FROM
        {{ ref('silver__token_transfers') }}
),
labels AS (
    SELECT
        contract_address,
        NAME,
        symbol,
        decimals
    FROM
        {{ ref('silver__ft_contract_metadata') }}
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
    amount_raw,
    amount_raw_precise,
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
    {{ dbt_utils.generate_surrogate_key(
        ['transfers_id']
    ) }} AS fact_token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    token_transfers t
LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
    ON t.contract_address = p.token_address
    AND DATE_TRUNC(
        'hour',
        t.block_timestamp
    ) = HOUR
LEFT JOIN labels C USING (contract_address)
{% if is_incremental() %}
WHERE
    GREATEST(
        t.modified_timestamp,
        COALESCE(
            b.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            f.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            s.modified_timestamp,
            '2000-01-01'
        )
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