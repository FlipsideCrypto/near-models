{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_token_prices_id',
    cluster_by = ['HOUR::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(token_address,symbol);",
    tags = ['scheduled_non_core']
) }}

WITH complete_token_prices AS (

    SELECT
        HOUR,
        LOWER(
            p.token_address
        ) AS token_address,
        asset_id,
        symbol,
        NAME,
        decimals,
        price,
        blockchain,
        blockchain_name,
        blockchain_id,
        is_imputed,
        is_deprecated,
        provider,
        source,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp,
        complete_token_prices_id,
        _invocation_id
    FROM
        {{ ref(
            'bronze__complete_token_prices'
        ) }}
        p

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY HOUR,
    token_address,
    symbol
    ORDER BY
        price ASC
) = 1
)
SELECT
    HOUR,
    token_address,
    asset_id,
    p.symbol,
    p.NAME,
    COALESCE(
        ft.decimals,
        p.decimals
    ) AS decimals,
    price,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    provider,
    source,
    p._inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['source', 'HOUR', 'token_address', 'p.symbol','blockchain', 'price']
    ) }} AS complete_token_prices_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    complete_token_prices p
    LEFT JOIN {{ ref('silver__ft_contract_metadata') }}
    ft
    ON p.token_address = ft.contract_address
