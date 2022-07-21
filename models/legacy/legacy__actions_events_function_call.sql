{{ config(
    materialized = 'view',
    secure = true
) }}

WITH actions_events_function_call AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call') }}
)
SELECT
    action_id,
    tx_hash AS txn_hash,
    block_timestamp,
    action_name,
    method_name,
    args,
    deposit,
    attached_gas,
    _ingested_at AS ingested_at
FROM
    actions_events_function_call
