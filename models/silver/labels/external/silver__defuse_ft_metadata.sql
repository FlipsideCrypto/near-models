{{ config(
    materialized = 'incremental',
    unique_key = 'defuse_ft_metadata_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}

WITH api_call AS (

    SELECT
        response
    FROM
        {{ ref('streamline__defuse_token_ids_realtime') }}
),
flattened AS (
SELECT
    VALUE :defuse_asset_identifier :: STRING AS defuse_asset_identifier,
    VALUE :intents_token_id :: STRING AS intents_token_id,
    VALUE :standard :: STRING AS standard,
    VALUE :asset_name :: STRING AS asset_name,
    VALUE :decimals :: INT AS decimals,
    VALUE :min_deposit_amount :: STRING AS min_deposit_amount,
    VALUE :min_withdrawal_amount :: STRING AS min_withdrawal_amount,
    VALUE :near_token_id :: STRING AS near_token_contract,
    VALUE :withdrawal_fee :: STRING AS withdrawal_fee
FROM
    api_call,
    LATERAL FLATTEN(
        input => response :data :result :tokens :: ARRAY
    )
),
chain_mapping AS (
    -- Map EVM chain IDs to blockchain names
    SELECT
        chain_id :: STRING AS chain_id,
        LOWER(chain) AS blockchain_name
    FROM
        {{ ref('silver__chainlist_ids') }}

),
parsed AS (
    SELECT
        defuse_asset_identifier,
        intents_token_id,
        standard,
        asset_name,
        decimals,
        min_deposit_amount,
        min_withdrawal_amount,
        near_token_contract,
        withdrawal_fee,
        -- Parse the asset_identifier (what ez_intents joins on)
        CASE
            WHEN standard = 'nep245' THEN
                -- For NEP245: extract everything after "nep245:"
                -- Example: nep245:v2_1.omni.hot.tg:56_11111111111111111111 -> v2_1.omni.hot.tg:56_11111111111111111111
                REGEXP_SUBSTR(intents_token_id, 'nep245:(.*)', 1, 1, 'e', 1)
            ELSE
                -- For NEP141: use near_token_contract as before
                near_token_contract
        END AS asset_identifier,
        -- Parse source_chain
        CASE
            WHEN standard = 'nep245' THEN
                -- For NEP245: parse from defuse_asset_identifier
                -- Format: blockchain:chainId:contractAddress
                COALESCE(
                    cm.blockchain_name,
                    CASE
                        WHEN SPLIT_PART(defuse_asset_identifier, ':', 1) = 'ton' THEN 'ton'
                        WHEN SPLIT_PART(defuse_asset_identifier, ':', 1) = 'sol' THEN 'sol'
                        WHEN SPLIT_PART(defuse_asset_identifier, ':', 1) = 'stellar' THEN 'stellar'
                        ELSE 'unknown'
                    END
                )
            WHEN SPLIT_PART(defuse_asset_identifier, ':', 1) = 'near' THEN 'near'
            WHEN SPLIT_PART(defuse_asset_identifier, ':', ARRAY_SIZE(SPLIT(defuse_asset_identifier, ':'))) = 'native' THEN
                SPLIT_PART(near_token_contract, '.', 1) :: STRING
            ELSE
                SPLIT_PART(near_token_contract, '-', 1) :: STRING
        END AS source_chain,
        -- Parse crosschain_token_contract
        CASE
            WHEN standard = 'nep245' THEN
                -- For NEP245: parse contract address from defuse_asset_identifier
                CASE
                    WHEN SPLIT_PART(defuse_asset_identifier, ':', ARRAY_SIZE(SPLIT(defuse_asset_identifier, ':'))) = 'native' THEN 'native'
                    ELSE SPLIT_PART(defuse_asset_identifier, ':', ARRAY_SIZE(SPLIT(defuse_asset_identifier, ':')))
                END
            ELSE
                SPLIT_PART(defuse_asset_identifier, ':', ARRAY_SIZE(SPLIT(defuse_asset_identifier, ':')))
        END AS crosschain_token_contract
    FROM
        flattened
    LEFT JOIN chain_mapping cm
        ON SPLIT_PART(flattened.defuse_asset_identifier, ':', 2) = cm.chain_id
        AND flattened.standard = 'nep245'
)
SELECT
    defuse_asset_identifier,
    asset_identifier,
    source_chain,
    crosschain_token_contract,
    asset_name,
    decimals,
    min_deposit_amount,
    min_withdrawal_amount,
    near_token_contract,
    withdrawal_fee,
    {{ dbt_utils.generate_surrogate_key(
        ['asset_identifier']
    ) }} AS defuse_ft_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    parsed

qualify(row_number() over (partition by asset_identifier order by inserted_timestamp asc)) = 1
