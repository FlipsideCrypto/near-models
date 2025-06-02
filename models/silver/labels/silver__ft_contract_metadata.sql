-- depends on: {{ ref('bronze__nearblocks_ft_metadata')}}
-- depends on: {{ ref('bronze__omni_metadata')}}
-- depends on: {{ ref('silver__defuse_tokens_metadata')}}

{{ config(
    materialized = 'incremental',
    unique_key = 'ft_contract_metadata_id',
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
),
omni AS (
    SELECT
        omni_asset_identifier AS asset_identifier,
        SPLIT_PART(omni_asset_identifier, ':', 1) :: STRING AS source_chain,
        SPLIT_PART(omni_asset_identifier, ':', 2) :: STRING AS crosschain_token_contract,
        contract_address AS near_token_contract
    FROM
        {{ ref('silver__omni_metadata')}}

    {% if is_incremental() %}
    WHERE
        modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
omni_unmapped AS (
    SELECT
        contract_address AS asset_identifier,
        SPLIT_PART(contract_address, ':', 1) :: STRING AS source_chain,
        SPLIT_PART(contract_address, ':', 2) :: STRING AS crosschain_token_contract
    FROM
        {{ ref('streamline__omni_tokenlist')}}
    WHERE
        source_chain_id IN ('near', 'sol')

    {% if is_incremental() %}
    WHERE
        modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
defuse AS (
    SELECT
        defuse_asset_identifier AS asset_identifier,
        CASE
            WHEN SPLIT_PART(defuse_asset_identifier, ':', 0) = 'near' THEN 'near'
            WHEN SPLIT_PART(defuse_asset_identifier, ':', ARRAY_SIZE(SPLIT(defuse_asset_identifier, ':'))) = 'native' THEN SPLIT_PART(near_token_id, '.', 0) :: STRING
            ELSE SPLIT_PART(near_token_id, '-', 0) :: STRING
        END AS source_chain,
        SPLIT_PART(defuse_asset_identifier, ':', ARRAY_SIZE(SPLIT(defuse_asset_identifier, ':'))) AS crosschain_token_contract,
        near_token_id AS near_token_contract,   
        asset_name AS symbol,
        decimals
    FROM
        {{ ref('silver__defuse_tokens_metadata')}}

    {% if is_incremental() %}
    WHERE
        modified_timestamp >= (
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
        o.asset_identifier,
        o.source_chain,
        o.crosschain_token_contract,
        o.near_token_contract,
        n.decimals :: INT AS decimals,
        n.name :: STRING AS name,
        n.symbol :: STRING AS symbol,
        'omni' AS source
    FROM
        omni o
    LEFT JOIN nearblocks_metadata n
        ON o.near_token_contract = n.near_token_contract
    
    UNION ALL

    -- Omni unmapped
    SELECT
        o.asset_identifier,
        o.source_chain,
        o.crosschain_token_contract,
        n.near_token_contract,
        COALESCE(n.decimals, c.decimals) :: INT AS decimals,
        COALESCE(n.name, c.name) :: STRING AS name,
        COALESCE(n.symbol, c.symbol) :: STRING AS symbol,
        'omni_unmapped' AS source
    FROM
        omni_unmapped o
    LEFT JOIN nearblocks_metadata n
        ON o.crosschain_token_contract = n.near_token_contract
    LEFT JOIN {{ source('crosschain_silver', 'complete_token_asset_metadata')}} c
        ON o.crosschain_token_contract = c.token_address
        AND c.blockchain = 'solana'
        -- note, this does not give use the Near token contract.
        -- could join on symbol, but some symbols have multiple contract records as symbol is not unique

    UNION ALL

    -- Nearblocks
    SELECT
        n.near_token_contract :: STRING AS asset_identifier,
        'near' AS source_chain,
        n.near_token_contract :: STRING AS crosschain_token_contract,
        n.near_token_contract,
        n.decimals :: INT AS decimals,
        n.name :: STRING AS name,
        n.symbol :: STRING AS symbol,
        'nearblocks' AS source
    FROM 
        nearblocks_metadata n

    UNION ALL

    -- Defuse
    SELECT
        d.asset_identifier,
        d.source_chain,
        d.crosschain_token_contract,
        d.near_token_contract,
        d.decimals :: INT AS decimals,
        n.name :: STRING AS name,
        d.symbol :: STRING AS symbol,
        'defuse' AS source
    FROM 
        defuse d
    LEFT JOIN nearblocks_metadata n
        ON d.near_token_contract = n.near_token_contract
)
SELECT
    asset_identifier,
    source_chain,
    crosschain_token_contract,
    near_token_contract,
    decimals,
    name,
    symbol,
    source as metadata_source,
    {{ dbt_utils.generate_surrogate_key(['asset_identifier']) }} AS ft_contract_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM final

qualify ROW_NUMBER() over (
    PARTITION BY asset_identifier
    ORDER BY modified_timestamp DESC
) = 1
