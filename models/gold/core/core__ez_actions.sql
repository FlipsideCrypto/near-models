{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_hash,
    tx_succeeded,
    tx_receiver,
    tx_signer,
    tx_gas_used,
    receipt_id,
    receipt_predecessor_id,
    receipt_receiver_id,
    receipt_signer_id,
    receipt_succeeded,
    receipt_gas_burnt,
    receipt_status_value,
    action_index,
    is_delegated,
    action_name,
    action_data,
    action_gas_price,
    actions_id AS fact_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__actions') }}
