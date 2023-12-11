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
    COALESCE(
        social_widgets_id,
        {{ dbt_utils.generate_surrogate_key(
            ['action_id_social']
        ) }}
    ) AS fact_widget_deployments_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    {{ ref('silver_social__widgets') }}
