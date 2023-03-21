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
    signer_id
FROM
    {{ ref('silver_social__addkey') }}
