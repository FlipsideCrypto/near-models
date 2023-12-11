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
    attached_gas,
    COALESCE(
        actions_events_function_call_id,
        {{ dbt_utils.generate_surrogate_key(
            ['action_id']
        ) }}
    ) AS fact_actions_events_function_call_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    actions_events_function_call
