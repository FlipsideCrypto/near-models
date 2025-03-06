-- depends on: {{ ref('seeds__ft_token_details_final')}}
-- depends on: {{ ref('bronze__nearblocks_ft_metadata')}}

{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}


WITH bronze AS (
    {% if var('NEAR_MIGRATE_ARCHIVE', false) %}

    SELECT
        contract_address,
        TRY_PARSE_JSON(DATA) AS DATA
    FROM
        {{ ref('seeds__ft_token_details_final')}}

    {% else %}

    SELECT
        VALUE :CONTRACT_ADDRESS :: STRING AS contract_address,
        DATA
    FROM
        {{ ref('bronze__nearblocks_ft_metadata')}}
    WHERE
        typeof(DATA) != 'NULL_VALUE'

    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
{% endif %}

),
flatten_results AS (
    SELECT
        VALUE :contract :: STRING AS contract_address,
        VALUE :decimals :: INT AS decimals,
        VALUE :name :: STRING AS NAME,
        VALUE :symbol :: STRING AS symbol,
        VALUE AS DATA
    FROM
        bronze,
        LATERAL FLATTEN(
            input => DATA :contracts
        )
)
SELECT
    contract_address,
    decimals,
    NAME,
    symbol,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS ft_contract_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_results

qualify ROW_NUMBER() over (
    PARTITION BY contract_address
    ORDER BY
        modified_timestamp DESC
) = 1
