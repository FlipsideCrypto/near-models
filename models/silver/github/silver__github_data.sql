{{ config(
    materialized = 'incremental',
    unique_key = ['_res_id'],
    cluster_by = ['_inserted_timestamp::DATE'],
    full_refresh = false,
    tags = ['activity']
) }}

SELECT
    repo_owner,
    repo_name,
    endpoint_name,
    data,
    provider,
    endpoint_github,
    _inserted_timestamp,
    _res_id
FROM
    {{ source(
        'crosschain_silver',
        'github_activity'
    ) }}
WHERE project_name = 'near'
{% if is_incremental() %}
AND
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}