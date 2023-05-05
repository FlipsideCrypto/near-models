{{ config(
    materialized = 'table',
    cluster_by = 'wallet_address',
    unique_key = 'wallet_address'
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
    'Near Foundation' AS creator
FROM
    labels
