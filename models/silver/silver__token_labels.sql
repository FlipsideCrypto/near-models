{{ config(
    materialized = 'table',
    unique_key = 'token_contract'
) }}

WITH labels_seed AS (

    SELECT
        *
    FROM
        {{ ref('seeds__token_labels') }}
)
SELECT
    *
FROM
    labels_seed
