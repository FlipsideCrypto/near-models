{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = ['_res_id'],
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['activity']
) }}

WITH github AS (

    SELECT
        repo_owner,
        repo_name,
        endpoint_name,
        DATA,
        provider,
        endpoint_github,
        _inserted_timestamp,
        _res_id
    FROM
        {{ source(
            'crosschain_silver',
            'github_activity'
        ) }}
    WHERE
        project_name = 'near'

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['_res_id']
    ) }} AS github_data_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    github
