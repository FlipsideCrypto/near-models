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
)
SELECT
    defuse_asset_identifier,
    CASE
        WHEN SPLIT_PART(defuse_asset_identifier, ':', 0) = 'near' THEN 'near'
        WHEN SPLIT_PART(defuse_asset_identifier, ':', ARRAY_SIZE(SPLIT(defuse_asset_identifier, ':'))) = 'native' THEN SPLIT_PART(near_token_contract, '.', 0) :: STRING
        ELSE SPLIT_PART(near_token_contract, '-', 0) :: STRING
    END AS source_chain,
    SPLIT_PART(defuse_asset_identifier, ':', ARRAY_SIZE(SPLIT(defuse_asset_identifier, ':'))) AS crosschain_token_contract,
    asset_name,
    decimals,
    min_deposit_amount,
    min_withdrawal_amount,
    near_token_contract,
    withdrawal_fee,
    {{ dbt_utils.generate_surrogate_key(
        ['defuse_asset_identifier']
    ) }} AS defuse_ft_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flattened

qualify(row_number() over (partition by defuse_asset_identifier order by inserted_timestamp asc)) = 1
