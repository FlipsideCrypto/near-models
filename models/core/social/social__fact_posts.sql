{{ config(
    materialized = 'view',
    tags = ['s3_curated', 'social']
) }}

SELECT
    tx_hash,
    action_id_social,
    block_id,
    block_timestamp,
    signer_id,
    post_type,
    post_text,
    post_image
FROM
    {{ ref('silver_social__posts') }}
