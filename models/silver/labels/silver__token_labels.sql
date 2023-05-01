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
    *
FROM
    labels_seed
