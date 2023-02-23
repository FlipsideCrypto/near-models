{{ config(
    materialized = 'view',
    tags = ['s3_curated', 'social']
) }}

SELECT
    action_id,
    tx_hash,
    block_id,
    block_timestamp,
    allowance,
    signer_id
FROM
    {{ ref('silver_social__addkey') }}
