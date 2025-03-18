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
    COALESCE(receipt_id, receipt_object_id) AS receipt_id,
    predecessor_id,
    receiver_id,
    signer_id,
    log_index,
    clean_log,
    iff(is_standard, try_parse_json(clean_log) :standard ::string, null) AS event_standard,
    gas_burnt,
    receipt_succeeded,
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
