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
    action_data,
    COALESCE(
        actions_events_id,
        {{ dbt_utils.generate_surrogate_key(
            ['receipt_object_id', 'action_index']
        ) }}
    ) AS fact_actions_events_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    actions
