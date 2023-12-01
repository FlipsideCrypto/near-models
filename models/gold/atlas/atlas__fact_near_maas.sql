{{ config(
    materialized = 'view',
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'ATLAS'
            }
        }
    },
    tags = ['atlas']
) }}

SELECT
    atlas_near_maa_id AS fact_near_maas_id,
    day,
    maa,
    new_maas,
    returning_maas,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__atlas_near_maa') }}
