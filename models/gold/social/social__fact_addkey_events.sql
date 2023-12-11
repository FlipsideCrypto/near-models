{{ config(
    materialized = 'view',
    tags = ['core', 'social']
) }}

SELECT
    action_id,
    tx_hash,
    block_id,
    block_timestamp,
    allowance,
    signer_id,
    COALESCE(
        social_addkey_id,
        {{ dbt_utils.generate_surrogate_key(
            ['action_id']
        ) }}
    ) AS fact_addkey_events_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    {{ ref('silver_social__addkey') }}
