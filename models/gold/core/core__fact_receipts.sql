{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    outcome_json :outcome :receipt_ids :: ARRAY AS receipt_outcome_id, -- TODO DEPRECATE THIS, it's in outcome_json
    receiver_id,
    predecessor_id,
    receipt_json AS actions, -- TODO this should be renamed. It's not just actions, it's the full receipt input
    outcome_json AS outcome,
    outcome_json :outcome :gas_burnt :: NUMBER AS gas_burnt,
    outcome_json :outcome :status :: VARIANT AS status_value,
    outcome_json :outcome :logs :: ARRAY AS logs,
    outcome_json :proof :: ARRAY AS proof, -- TODO DEPRECATE THIS, it's in outcome_json
    outcome_json :outcome :metadata :: VARIANT AS metadata, -- TODO DEPRECATE THIS, it's in outcome_json
    receipt_succeeded,
    receipts_final_id AS fact_receipts_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__receipts_final') }}
