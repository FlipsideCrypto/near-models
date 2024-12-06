{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions') }}
)
SELECT
    block_id,
    block_timestamp,
    fa.tx_hash,
    tx_succeeded,
    tx_receiver,
    tx_signer,
    tx_gas_used,
    fa.receipt_id,
    receipt_predecessor_id,
    receipt_receiver_id,
    receipt_signer_id,
    receipt_succeeded,
    receipt_gas_burnt,
    receipt_status_value,
    action_index,
    action_name,
    action_data_parsed AS action_data,
    action_gas_price,
    receipt_logs,
    actions_id AS fact_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    actions