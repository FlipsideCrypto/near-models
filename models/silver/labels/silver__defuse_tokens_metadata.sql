{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'defuse_asset_identifier',
    tags = ['scheduled_non_core']
) }}

WITH api_call AS (

    SELECT
        response
    FROM
        {{ ref('streamline__defuse_token_ids_realtime') }}
)
SELECT
    VALUE :asset_name :: STRING AS asset_name,
    VALUE :decimals :: INT AS decimals,
    VALUE :defuse_asset_identifier :: STRING AS defuse_asset_identifier,
    VALUE :min_deposit_amount :: STRING AS min_deposit_amount,
    VALUE :min_withdrawal_amount :: STRING AS min_withdrawal_amount,
    VALUE :near_token_id :: STRING AS near_token_id,
    VALUE :withdrawal_fee :: STRING AS withdrawal_fee,
    {{ dbt_utils.generate_surrogate_key(
        ['defuse_asset_identifier']
    ) }} AS defuse_token_ids_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    api_call,
    LATERAL FLATTEN(
        input => response :data :result :tokens :: ARRAY
    )

qualify(row_number() over (partition by defuse_asset_identifier order by inserted_timestamp asc)) = 1
