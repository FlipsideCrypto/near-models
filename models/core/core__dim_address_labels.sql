{{ config(
    materialized = 'view',
    secure = true
) }}

SELECT
    *
FROM
    {{ source(
        'gold',
        'near_address_labels'
    ) }}
