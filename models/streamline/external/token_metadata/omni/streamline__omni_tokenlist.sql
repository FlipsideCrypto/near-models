{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate","modified_timestamp::date"],
    unique_key = "contract_address",
    cluster_by = ['modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['streamline_non_core']
) }}

WITH omni_token AS (
    SELECT
        DISTINCT lower(raw_token_id) AS contract_address
    FROM 
        {{ ref('silver__bridge_omni') }}
    WHERE
        source_chain_id NOT IN ('near', 'sol')
    
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01')
    FROM
        {{ this }})
{% endif %}
)
SELECT
    contract_address,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS omni_tokenlist_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    omni_token
QUALIFY (ROW_NUMBER() OVER (PARTITION BY contract_address
    ORDER BY
        modified_timestamp ASC) = 1)
