{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_token_prices_id',
    cluster_by = ['HOUR::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(token_address,symbol);",
    tags = ['scheduled_non_core', 'grail']
) }}

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
QUALIFY row_number() OVER (partition by HOUR, token_address, symbol ORDER BY price ASC) = 1
