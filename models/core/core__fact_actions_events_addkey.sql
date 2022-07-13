{{ config(
    materialized = 'view'
) }}

WITH actions_events_addkey AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_addkey') }}
)
SELECT
    action_id,
    tx_hash,
    block_timestamp,
    nonce,
    public_key,
    permission,
    allowance,
    method_name,
    receiver_id
FROM
    actions_events_addkey
