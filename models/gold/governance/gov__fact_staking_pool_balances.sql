{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core', 'governance'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING, GOVERNANCE' }}}
) }}

WITH balance_changes AS (

    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id AS address,
        amount_adj AS balance,
        COALESCE(
            pool_balances_id,
            {{ dbt_utils.generate_surrogate_key(
                ['tx_hash']
            ) }}
        ) AS fact_staking_pool_balances_id,
        COALESCE(inserted_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__pool_balances') }}
)
SELECT
    *
FROM
    balance_changes
