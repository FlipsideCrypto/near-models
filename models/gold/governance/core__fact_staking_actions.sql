{{ config(
    materialized = 'view',
    secure = true,
    tags = ['core', 'governance'],
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'STAKING GOVERNANCE'
            }
        }
    }
) }}

WITH staking_actions AS (

    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id AS address,
        signer_id,
        action,
        amount_adj AS amount
    FROM
        {{ ref('silver__staking_actions_v2') }}
)
SELECT
    *
FROM
    staking_actions
