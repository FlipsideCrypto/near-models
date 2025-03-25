{{ config(
    materialized = 'view',
    secure = false,
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'ATLAS'
            }
        }
    },
    tags = ['scheduled_core']
) }}

WITH supply AS (

    SELECT
        utc_date,
        total_supply,
        total_staked_supply,
        total_nonstaked_supply,
        circulating_supply,
        total_locked_supply,
        liquid_supply,
        nonliquid_supply,
        staked_locked_supply,
        non_staked_locked_supply,
        staked_circulating_supply,
        nonstaked_circulating_supply,
        perc_locked_supply,
        perc_circulating_supply,
        perc_staked_locked,
        perc_staked_circulating,
        atlas_supply_id AS ez_supply_id,
        COALESCE(inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__atlas_supply') }}
)
SELECT
    *
FROM
    supply
