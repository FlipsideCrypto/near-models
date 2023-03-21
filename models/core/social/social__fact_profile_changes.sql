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
    profile_data
FROM
    {{ ref('silver_social__profile_changes') }}
