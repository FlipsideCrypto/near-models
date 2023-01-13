{{ config(
    materialized = 'view',
    secure = true
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}
)
SELECT
    block_id,
    block_timestamp,
    block_hash,
    tx_count,
    block_author,
    block_challenges_result,
    block_challenges_root,
    chunk_headers_root,
    chunk_tx_root,
    chunk_mask,
    chunk_receipts_root,
    chunks,
    chunks_included,
    epoch_id,
    epoch_sync_data_hash,
    events,
    gas_price,
    last_ds_final_block,
    last_final_block,
    latest_protocol_version,
    next_bp_hash,
    next_epoch_id,
    outcome_root,
    prev_hash,
    prev_height,
    prev_state_root,
    random_value,
    rent_paid,
    signature,
    total_supply,
    validator_proposals,
    validator_reward
FROM
    blocks
