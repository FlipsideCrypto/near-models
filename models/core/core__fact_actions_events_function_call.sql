{{ config(
    materialized = 'view'
) }}

WITH actions_events_function_call AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call') }}
)
SELECT
    action_id,
    txn_hash,
    block_timestamp,
    action_name,
    method_name,
    args,
    deposit,
    attached_gas
FROM
    actions_events_function_call
