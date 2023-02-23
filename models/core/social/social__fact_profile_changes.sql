{{ config(
    materialized = 'view',
    tags = ['s3_curated', 'social']
) }}

SELECT
    action_id_profile,
    tx_hash,
    block_id,
    block_timestamp,
    signer_id,
    profile_section,
    profile_data
FROM
    {{ ref('silver_social__profile_changes') }}
