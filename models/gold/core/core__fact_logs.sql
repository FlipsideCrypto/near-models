{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH logs AS (

    SELECT
        *
    FROM
        {{ ref('silver__logs_s3') }}
)
SELECT
    tx_hash,
    block_timestamp,
    block_id,
    receiver_id,
    signer_id,
    clean_log,
    receipt_object_id,
    gas_burnt,
    COALESCE(
        logs_id,
        {{ dbt_utils.generate_surrogate_key(
            ['action_id']
        ) }}
    ) AS fact_logs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    logs
