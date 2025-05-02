{{ config(
    materialized = 'view',
    secure = false,
    tags = ['scheduled_non_core'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING, GOVERNANCE' }}}
) }}

WITH lockup_actions AS (

    SELECT
        tx_hash,
        block_timestamp,
        block_id,
        deposit,
        lockup_account_id,
        owner_account_id,
        lockup_duration,
        lockup_timestamp,
        lockup_timestamp_ntz,
        release_duration,
        vesting_schedule,
        transfers_information,
        COALESCE(
            lockup_actions_id,
            {{ dbt_utils.generate_surrogate_key(
                ['tx_hash']
            ) }}
        ) AS fact_lockup_actions_id,
        COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__lockup_actions') }}
)
SELECT
    *
FROM
    lockup_actions
