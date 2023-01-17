{{ config(
    materialized = 'view',
    secure = true
) }}

WITH actions_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_s3') }}
)
SELECT
    action_id,
    tx_hash,
    block_id,
    block_timestamp,
    action_index,
    action_name,
    action_data
FROM
    actions_events
