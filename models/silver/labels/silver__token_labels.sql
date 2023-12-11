{{ config(
    materialized = 'view',
    unique_key = 'token_contract'
) }}

WITH labels_seed AS (

    SELECT
        token,
        symbol,
        token_contract,
        decimals
    FROM
        {{ ref('seeds__token_labels') }}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['token_contract']
    ) }} AS token_labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    labels_seed
