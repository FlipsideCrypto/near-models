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
    widget_name,
    source_code,
    metadata,
    branch,
    widget_modules_used,
    widget_url
FROM
    {{ ref('silver_social__widgets') }}
