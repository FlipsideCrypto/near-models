{{ config(
    materialized = 'incremental',
    
    tags = ['intents', 'curated', 'scheduled_non_core']
) }}
WITH intents AS (
    SELECT
        *
    FROM {{ ref('silver__intents') }}
)
SELECT         
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    log_event,
    log_index,
    log_event_index,
    owner_id,
    old_owner_id,
    new_owner_id,
    amount_index,
    amount,
    token_id as raw_token_id,
    gas_burnt,
    memo,
    receipt_succeeded,
    COALESCE(
        intent_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'receipt_id', 'log_index']  
        ) }}
    ) AS intent_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM 
    intents