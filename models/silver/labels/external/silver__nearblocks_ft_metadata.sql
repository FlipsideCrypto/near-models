{{ config(
    materialized = 'incremental',
    unique_key = 'nearblocks_ft_metadata_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}



WITH nearblocks AS (

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
),
nearblocks_metadata AS (
    SELECT
        VALUE :contract :: STRING AS near_token_contract,
        VALUE :decimals :: INT AS decimals,
        VALUE :name :: STRING AS NAME,
        VALUE :symbol :: STRING AS symbol
    FROM
        nearblocks,
        LATERAL FLATTEN(
            input => DATA :contracts
        )
)
SELECT
    near_token_contract,
    decimals,
    name,
    symbol,
    CASE
        WHEN near_token_contract ilike 'sol-%.omft.near' THEN 'sol'
        WHEN near_token_contract rlike '.*-0x[0-9a-fA-F]+\\.(duse|omft)\\.near' THEN SPLIT_PART(near_token_contract, '-', 1) :: STRING
        WHEN near_token_contract ilike '%-native.duse.near' THEN SPLIT_PART(near_token_contract, '-', 1) :: STRING
        ELSE 'near'
    END AS source_chain,
    IFF(
        source_chain = 'near',
        near_token_contract,
        COALESCE(
            REGEXP_SUBSTR(near_token_contract, '0x[a-fA-F0-9]{40}') :: STRING,
            REGEXP_SUBSTR(near_token_contract, 'native') :: STRING,
            REGEXP_SUBSTR(near_token_contract, 'sol-([^.]+)\\.omft\\.near', 1, 1, 'e', 1)
        )
    ) AS crosschain_token_contract,
    {{ dbt_utils.generate_surrogate_key(
        ['near_token_contract']
    ) }} AS nearblocks_ft_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM nearblocks_metadata

qualify(row_number() over (partition by near_token_contract order by modified_timestamp desc)) = 1
