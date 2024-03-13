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
    block_id,
    block_timestamp,
    tx_hash,
    receipt_object_id,
    predecessor_id,
    receiver_id,
    signer_id,
    clean_log,
    gas_burnt,
    COALESCE(
        logs_id,
        {{ dbt_utils.generate_surrogate_key(
            ['log_id']
        ) }}
    ) AS fact_logs_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    logs
