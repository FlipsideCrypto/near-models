{{ config(
    materialized = 'view',
    secure = true,
    tags = ['core', 'governance'],
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'STAKING, GOVERNANCE'
            }
        }
    }
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
        transfers_information
    FROM
        {{ ref('silver__lockup_actions') }}
)
SELECT
    *
FROM
    lockup_actions
