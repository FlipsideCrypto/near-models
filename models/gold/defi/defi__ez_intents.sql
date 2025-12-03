{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core', 'intents']
) }}

-- Backward compatibility view for defi__ez_intents
-- Now sources from intents__ez_transactions to avoid duplicate processing

SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    token_id,
    symbol,
    amount_adj,
    amount_usd,
    owner_id,
    old_owner_id,
    new_owner_id,
    amount_raw,
    blockchain,
    contract_address,
    is_native,
    price,
    decimals,
    gas_burnt,
    memo,
    referral,
    fees_collected_raw,
    fee_token,
    fee_amount_adj,
    fee_amount_usd,
    dip4_version,
    log_index,
    log_event_index,
    amount_index,
    receipt_succeeded,
    token_is_verified,
    ez_transactions_id AS ez_intents_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    {{ ref('intents__ez_transactions') }}
