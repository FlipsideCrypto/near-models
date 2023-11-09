{{ config(
    materialized = 'view',
    tags = ['atlas']
) }}

SELECT
    atlas_near_maa_id AS fact_near_maas_id,
    active_day,
    maa,
    new_maas,
    returning_maas,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__atlas_near_maa') }}
