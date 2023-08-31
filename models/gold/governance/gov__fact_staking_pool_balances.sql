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

WITH balance_changes AS (

    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id AS address,
        amount_adj AS balance
    FROM
        {{ ref('silver__pool_balances') }}
)
SELECT
    *
FROM
    balance_changes
