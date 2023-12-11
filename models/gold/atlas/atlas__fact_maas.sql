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
    atlas_maa_id AS fact_maas_id,
    day,
    maa,
    new_maas,
    returning_maas,
    COALESCE(inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    {{ ref('silver__atlas_maa') }}
