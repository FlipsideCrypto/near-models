{{ config(
    materialized = 'view',
    tags = ['atlas']
) }}

SELECT
    active_day,
    maa,
    new_maas,
    returning_maas,
    inserted_timestamp,
    modified_timestamp,
    invocation_id
FROM
    {{ ref('silver__atlas_near_maa') }}
