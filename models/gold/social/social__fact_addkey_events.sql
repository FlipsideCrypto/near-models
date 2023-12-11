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
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_social__addkey') }}
