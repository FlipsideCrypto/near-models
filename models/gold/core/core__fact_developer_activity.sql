{{ config(
    materialized = 'view',
    tags = ['core', 'activity']
) }}

WITH github_data AS (

    SELECT
        *
    FROM
        {{ ref('silver__github_data') }}
)
SELECT
    repo_owner,
    repo_name,
    endpoint_name,
    DATA,
    provider,
    endpoint_github,
    _inserted_timestamp AS snapshot_timestamp,
    COALESCE(
        github_data_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_res_id']
        ) }}
    ) AS fact_developer_activity_id,
    COALESCE(inserted_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    github_data
