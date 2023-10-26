{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_s3') }}
)
SELECT
    action_id,
    tx_hash,
    receipt_object_id,
    receiver_id,
    signer_id,
    block_id,
    block_timestamp,
    action_index,
    action_name,
    action_data
FROM
    actions
