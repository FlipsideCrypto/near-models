{{ config(
    materialized = 'view',
    tags = ['core', 'social']
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
    widget_url,
    social_widgets_id AS fact_widget_deployments_id,
    inserted_timestamp AS inserted_timestamp,
    modified_timestamp AS modified_timestamp
FROM
    {{ ref('silver_social__widgets') }}
