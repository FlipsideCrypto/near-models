{{ config(
    materialized = 'view',
    secure = true
) }}

WITH actions_events_addkey AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_addkey_s3') }}
)
SELECT
    action_id,
    tx_hash,
    block_id,
    block_timestamp,
    nonce,
    public_key,
    permission,
    allowance,
    method_name,
    receiver_id
FROM
    actions_events_addkey
