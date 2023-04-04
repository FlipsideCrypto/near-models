{{ config(
    materialized = 'view',
    secure = true,
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'STAKING'
            }
        }
    },
    tags = ['core']
) }}

WITH staking_actions AS (

    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id AS pool_address,
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
