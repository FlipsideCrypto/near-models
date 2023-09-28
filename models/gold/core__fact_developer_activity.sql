{{ config(
    materialized = 'view',
    tags = ['core', 'livequery']
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
    data,
    provider,
    endpoint_github,
    _inserted_timestamp as inserted_at
FROM
    github_data