{{ config(
    materialized = 'table',
    cluster_by = 'wallet_address',
    unique_key = 'wallet_address',
    tags = ['scheduled_non_core']
) }}

WITH labels AS (

    SELECT
        project_name,
        category,
        wallet_address,
        enabled
    FROM
        {{ ref('seeds__near_project_labels_v1') }}
)
SELECT
    '2023-05-05' :: TIMESTAMP AS system_created_at,
    project_name,
    category,
    wallet_address,
    enabled,
    'Near Foundation' AS creator,
    {{ dbt_utils.generate_surrogate_key(
        ['wallet_address']
    ) }} AS foundation_labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    labels
