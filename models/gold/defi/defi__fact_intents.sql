{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core', 'intents']
) }}

-- Backward compatibility view for defi__fact_intents
-- Now sources from intents__fact_transactions to avoid duplicate processing

SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    log_index,
    log_event_index,
    owner_id,
    old_owner_id,
    new_owner_id,
    memo,
    amount_index,
    amount_raw,
    token_id,
    referral,
    fees_collected_raw,
    dip4_version,
    gas_burnt,
    receipt_succeeded,
    fact_transactions_id AS fact_intents_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    {{ ref('intents__fact_transactions') }}
