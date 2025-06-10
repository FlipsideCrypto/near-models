-- depends on: {{ ref('silver__nearblocks_ft_metadata')}}
-- depends on: {{ ref('silver__omni_ft_metadata')}}
-- depends on: {{ ref('silver__defuse_ft_metadata')}}

{{ config(
    materialized = 'incremental',
    unique_key = 'ft_contract_metadata_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}


WITH nearblocks AS (

    SELECT
        near_token_contract,
        decimals,
        name,
        symbol,
        source_chain,
        crosschain_token_contract,
        'nearblocks' AS source
    FROM
        {{ ref('silver__nearblocks_ft_metadata')}}

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
omni AS (
    SELECT
        omni_asset_identifier AS asset_identifier,
        o.source_chain,
        o.crosschain_token_contract,
        o.near_token_contract,
        n.decimals,
        n.name,
        n.symbol,
        'omni' AS source
    FROM
        {{ ref('silver__omni_ft_metadata')}} o
    
    {% if is_incremental() %}
    WHERE
        o.modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
omni_unmapped AS (
    SELECT
        omni_asset_identifier AS asset_identifier,
        o.source_chain,
        o.crosschain_token_contract,
        n.near_token_contract,
        COALESCE(n.decimals, c.decimals) :: INT AS decimals,
        COALESCE(n.name, c.name) :: STRING AS name,
        COALESCE(n.symbol, c.symbol) :: STRING AS symbol,
        'omni_unmapped' AS source
    FROM
        {{ ref('streamline__omni_tokenlist')}} o
    LEFT JOIN {{ source('crosschain_silver', 'complete_token_asset_metadata')}} c
        ON o.crosschain_token_contract = c.token_address
        AND c.blockchain = 'solana'
        -- note, this does not give use the Near token contract.
        -- could join on symbol, but some symbols have multiple contract records as symbol is not unique
    WHERE
        o.source_chain IN ('near', 'sol')

    {% if is_incremental() %}
    AND
        o.modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
defuse AS (
    SELECT
        d.near_token_contract AS asset_identifier,
        d.source_chain,
        d.crosschain_token_contract,
        d.near_token_contract,   
        d.decimals,
        n.name,
        asset_name AS symbol,
        'defuse' AS source
    FROM
        {{ ref('silver__defuse_ft_metadata')}} d

    {% if is_incremental() %}
    WHERE
        d.modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
final AS (
    -- Nearblocks
    SELECT
        near_token_contract AS asset_identifier,
        source_chain,
        crosschain_token_contract,
        near_token_contract,
        decimals,
        name,
        symbol,
        source
    FROM 
        nearblocks

    UNION ALL

    -- Omni
    SELECT
        asset_identifier,
        source_chain,
        crosschain_token_contract,
        near_token_contract,
        decimals,
        name,
        symbol,
        source
    FROM
        omni
    
    UNION ALL

    -- Omni unmapped
    SELECT
        asset_identifier,
        source_chain,
        crosschain_token_contract,
        near_token_contract,
        decimals,
        name,
        symbol,
        source
    FROM
        omni_unmapped

    UNION ALL

    -- Defuse
    SELECT
        asset_identifier,
        source_chain,
        crosschain_token_contract,
        near_token_contract,
        decimals,
        name,
        symbol,
        source
    FROM 
        defuse
),
final_joined AS (
     SELECT 
        f.asset_identifier,
        f.source_chain,
        f.crosschain_token_contract,
        f.near_token_contract,
        CASE 
            WHEN f.source = 'omni_unmapped' AND f.source_chain != 'near' THEN f.decimals
            ELSE COALESCE(f.decimals, n.decimals)
        END AS decimals,
        CASE 
            WHEN f.source = 'omni_unmapped' AND f.source_chain != 'near' THEN f.name
            ELSE COALESCE(f.name, n.name)
        END AS name,
        CASE 
            WHEN f.source = 'omni_unmapped' AND f.source_chain != 'near' THEN f.symbol
            ELSE COALESCE(f.symbol, n.symbol)
        END AS symbol,
        f.source AS metadata_provider
    FROM final f
    LEFT JOIN {{ ref('silver__nearblocks_ft_metadata') }} n
        ON f.near_token_contract = n.near_token_contract
        AND NOT (f.source = 'omni_unmapped' AND f.source_chain != 'near')
)
SELECT
    asset_identifier,
    source_chain,
    crosschain_token_contract,
    near_token_contract,
    decimals,
    name,
    symbol,
    source as metadata_provider,
    {{ dbt_utils.generate_surrogate_key(['asset_identifier']) }} AS ft_contract_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM final

qualify ROW_NUMBER() over (
    PARTITION BY asset_identifier
    ORDER BY 
        metadata_provider = 'defuse' DESC, -- prioritize defuse over nearblocks for those tokens
        modified_timestamp DESC
) = 1
