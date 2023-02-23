{{ config(
    materialized = 'view',
    tags = ['s3_curated', 'social']
) }}

SELECT
    action_id_social,
    tx_hash,
    block_id,
    block_timestamp,
    node,
    node_data
FROM
    {{ ref('silver_social__decoded_actions') }}
