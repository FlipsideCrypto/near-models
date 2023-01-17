{{ config(
    materialized = 'view',
    secure = true
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_blocks') }}
)
SELECT
    block_id,
    block_timestamp,
    block_hash,
    tx_count,
    block_author,
    header:block_challenges_result as block_challenges_result,
    header:block_challenges_root as block_challenges_root,
    header:chunk_headers_root as chunk_headers_root,
    header:chunk_tx_root as chunk_tx_root,
    header:chunk_mask as chunk_mask,
    header:chunk_receipts_root as chunk_receipts_root,
    chunks,
    header:chunks_included as chunks_included,
    epoch_id,
    header:epoch_sync_data_hash as epoch_sync_data_hash,
    events,
    gas_price,
    header:last_ds_final_block as last_ds_final_block,
    header:last_final_block as last_final_block,
    latest_protocol_version,
    header: next_bp_hash as next_bp_hash,
    next_epoch_id,
    header:outcome_root as outcome_root,
    prev_hash,
    header:prev_height as prev_height,
    header:prev_state_root as prev_state_root,
    header:random_value as random_value,
    header:rent_paid as rent_paid,
    header:signature as signature,
    total_supply,
    validator_proposals,
    validator_reward
FROM
    blocks
