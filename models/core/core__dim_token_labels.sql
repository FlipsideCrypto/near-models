{{ config(
    materialized = 'view',
    secure = true
) }}

WITH token_labels AS (

    SELECT
        *
    FROM
        {{ ref('silver__token_labels') }}
)
SELECT
    *
FROM
    token_labels
