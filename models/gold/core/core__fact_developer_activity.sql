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
    inserted_timestamp,
    modified_timestamp
FROM
    github_data
