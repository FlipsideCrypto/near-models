{{ config(
    materialized = 'view',
    tags = ['core', 'social']
) }}

SELECT
    action_id_social,
    tx_hash,
    block_id,
    block_timestamp,
    signer_id,
    node,
    node_data
FROM
    {{ ref('silver_social__decoded_actions') }}
