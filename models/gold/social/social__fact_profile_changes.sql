{{ config(
    materialized = 'view',
    tags = ['core', 'social']
) }}

SELECT
    action_id_profile,
    tx_hash,
    block_id,
    block_timestamp,
    signer_id,
    profile_section,
    profile_data,
    COALESCE(
        social_profile_changes_id,
        {{ dbt_utils.generate_surrogate_key(
            ['action_id_profile']
        ) }}
    ) AS fact_profile_changes_id,
    COALESCE(inserted_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    {{ ref('silver_social__profile_changes') }}
