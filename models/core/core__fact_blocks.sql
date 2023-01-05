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
    prev_hash,
    block_author,
    gas_price,
    total_supply,
    validator_proposals,
    validator_reward,
    latest_protocol_version,
    epoch_id,
    next_epoch_id,
    tx_count,
    events,
    chunks,
    header
FROM
    blocks
