{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate","modified_timestamp::date"],
    unique_key = "contract_address",
    cluster_by = ['modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['streamline_non_core']
) }}

WITH ft_transfers AS (

    SELECT
        DISTINCT lower(receipt_receiver_id) AS contract_address
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        action_name = 'FunctionCall'
        AND action_data :method_name = 'ft_transfer'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01')
    FROM
        {{ this }})
    {% endif %}
) 
{% if not is_incremental() %}
    {% do log('Is FR, loading from seed file', info=true) %}
,
    tokenlist_seed AS (
        SELECT
            lower(contract_address) AS contract_address
        FROM
            {{ ref('seeds__ft_tokenlist') }}
    )
{% endif %}
,
FINAL AS (
    SELECT
        contract_address
    FROM
        ft_transfers

    {% if not is_incremental() %}
    UNION
    SELECT
        contract_address
    FROM
        tokenlist_seed
    {% endif %}
)
SELECT
    contract_address,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS ft_tokenlist_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL

    qualify(ROW_NUMBER() over (PARTITION BY contract_address
    ORDER BY
        modified_timestamp ASC) = 1)
