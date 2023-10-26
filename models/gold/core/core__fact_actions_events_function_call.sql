{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH actions_events_function_call AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
)
SELECT
    action_id,
    tx_hash,
    receiver_id,
    signer_id,
    block_id,
    block_timestamp,
    action_name,
    method_name,
    args,
    deposit,
    attached_gas
FROM
    actions_events_function_call
