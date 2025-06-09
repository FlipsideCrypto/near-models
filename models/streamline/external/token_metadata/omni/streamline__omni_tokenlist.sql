{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "omni_asset_identifier",
    cluster_by = ['modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(omni_asset_identifier);",
    tags = ['streamline_non_core']
) }}

WITH omni_token AS (
    SELECT
        DISTINCT raw_token_id AS omni_asset_identifier
    FROM 
        {{ ref('silver__bridge_omni') }}
    
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01')
    FROM
        {{ this }})
{% endif %}
)
SELECT
    omni_asset_identifier,
    SPLIT_PART(omni_asset_identifier, ':', 1) :: STRING AS source_chain,
    SPLIT_PART(omni_asset_identifier, ':', 2) :: STRING AS crosschain_token_contract,
    {{ dbt_utils.generate_surrogate_key(
        ['omni_asset_identifier']
    ) }} AS omni_tokenlist_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    omni_token
QUALIFY (ROW_NUMBER() OVER (PARTITION BY omni_asset_identifier
    ORDER BY
        modified_timestamp ASC) = 1)
