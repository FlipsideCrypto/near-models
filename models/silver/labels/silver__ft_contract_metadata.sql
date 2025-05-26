-- depends on: {{ ref('bronze__nearblocks_ft_metadata')}}
-- depends on: {{ ref('bronze__omni_metadata')}}
-- depends on: {{ ref('silver__defuse_tokens_metadata')}}

{{ config(
    materialized = 'incremental',
    unique_key = ['omni_address', 'contract_address'],
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
        VALUE :contract :: STRING AS contract_address,
        VALUE :decimals :: INT AS decimals,
        VALUE :name :: STRING AS NAME,
        VALUE :symbol :: STRING AS symbol,
        VALUE AS DATA
    FROM
        nearblocks,
        LATERAL FLATTEN(
            input => DATA :contracts
        )
),
omni AS (
    SELECT
        omni_address,
        contract_address
    FROM
        {{ ref('silver__omni_metadata')}}

    {% if is_incremental() %}
    WHERE
        inserted_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
defuse AS (
    SELECT
        defuse_asset_identifier AS defuse_address,
        near_token_id AS contract_address,   
        asset_name AS symbol,
        decimals
    FROM
        {{ ref('silver__defuse_tokens_metadata')}}

    {% if is_incremental() %}
    WHERE
        inserted_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
final AS (
    -- Omni
    SELECT
        o.omni_address :: STRING AS omni_address,
        o.contract_address :: STRING AS contract_address,
        n.decimals :: INT AS decimals,
        n.name :: STRING AS name,
        n.symbol :: STRING AS symbol,
        'omni' AS source
    FROM
        omni o
    LEFT JOIN nearblocks_metadata n
        ON o.contract_address = n.contract_address
    
    UNION ALL

    -- Nearblocks
    SELECT
        NULL :: STRING AS omni_address,
        n.contract_address :: STRING AS contract_address,
        n.decimals :: INT AS decimals,
        n.name :: STRING AS name,
        n.symbol :: STRING AS symbol,
        'nearblocks' AS source
    FROM 
        nearblocks_metadata n
    WHERE n.contract_address NOT IN (SELECT contract_address FROM omni)

    UNION ALL

    -- Defuse
    SELECT
        d.defuse_address :: STRING AS omni_address,
        d.contract_address :: STRING AS contract_address,
        d.decimals :: INT AS decimals,
        NULL :: STRING AS name,
        d.symbol :: STRING AS symbol,
        'defuse' AS source
    FROM 
        defuse d
)
SELECT
    contract_address,
    omni_address,
    decimals,
    name,
    symbol,
    source,
    {{ dbt_utils.generate_surrogate_key(['omni_address', 'contract_address']) }} AS ft_contract_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM final

qualify ROW_NUMBER() over (
    PARTITION BY omni_address, contract_address
    ORDER BY modified_timestamp DESC
) = 1
