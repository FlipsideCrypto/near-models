{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core', 'governance'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING, GOVERNANCE' }}}
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
        amount_adj AS amount,
        COALESCE(
            staking_actions_v2_id,
            {{ dbt_utils.generate_surrogate_key(
                ['tx_hash']
            ) }}
        ) AS fact_staking_actions_id,
        COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__staking_actions_v2') }}
)
SELECT
    *
FROM
    staking_actions
