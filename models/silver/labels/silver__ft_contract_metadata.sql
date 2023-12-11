{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['livequery', 'nearblocks']
) }}

WITH livequery_results AS (

    SELECT
        *
    FROM
        {{ ref('livequery__request_nearblocks_ft_metadata') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
flatten_results AS (
    SELECT
        VALUE :contract :: STRING AS contract_address,
        VALUE :decimals :: INT AS decimals,
        VALUE :icon :: STRING AS icon,
        VALUE :name :: STRING AS NAME,
        VALUE :symbol :: STRING AS symbol,
        VALUE AS DATA,
        _inserted_timestamp,
        _res_id
    FROM
        livequery_results,
        LATERAL FLATTEN(
            input => DATA :data :tokens
        )
),
FINAL AS (
    SELECT
        contract_address,
        decimals,
        icon,
        NAME,
        symbol,
        DATA,
        _inserted_timestamp,
        _res_id
    FROM
        flatten_results 
        qualify ROW_NUMBER() over (
            PARTITION BY contract_address
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS ft_contract_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
