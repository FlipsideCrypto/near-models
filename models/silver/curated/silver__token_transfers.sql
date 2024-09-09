{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::Date'],
    unique_key = 'transfers_id',
    incremental_strategy = 'merge',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,action_id,contract_address,from_address,to_address);",
    tags = ['curated','scheduled_non_core']
) }}
{# Note - multisource model #}
-- Curation Challenge - 'https://flipsidecrypto.xyz/Hossein/transfer-sector-of-near-curation-challenge-zgM44F'

------------------------------   NEAR Tokens (NEP 141) --------------------------------
WITH orders_final AS (
    SELECT 
        *
    FROM
        {{ref('silver__token_transfer_orders')}}
),
add_liquidity AS (
    SELECT
        *
    FROM
        {{ref('silver__token_transfer_liquidity')}}
),
ft_transfers_method AS (
    SELECT
        *
    FROM
        {{ref('silver__token_transfer_ft_transfers_method')}}
),
ft_transfers_event AS (
    SELECT
        *
    FROM
        {{ref('silver__token_transfer_ft_transfers_event')}}
),
ft_mints_final AS (
    SELECT
        *
    FROM
        {{ref('silver__token_transfer_mints')}}
),
nep_transfers AS (
    SELECT
        *
    FROM
        ft_transfers_method
    UNION ALL
    SELECT
        *
    FROM
        ft_transfers_event
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        contract_address,
        from_address,
        to_address,
        amount_unadjusted,
        memo,
        rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        ft_mints_final
    UNION ALL
    SELECT
        *
    FROM
        orders_final
    UNION ALL
    SELECT
        *
    FROM
        add_liquidity
),
native_transfers AS (
    SELECT
        *
    FROM
        {{ref('silver__token_transfer_base_native')}}
),
------------------------------  MODELS --------------------------------
native_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        'wrap.near' AS contract_address,
        from_address :: STRING,
        to_address :: STRING,
        NULL AS memo,
        '0' AS rn,
        'native' AS transfer_type,
        amount_unadjusted :: STRING AS amount_raw,
        amount_unadjusted :: FLOAT AS amount_raw_precise,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        native_transfers
),
nep_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        contract_address,
        from_address,
        to_address,
        memo,
        rn :: STRING AS rn,
        'nep141' AS transfer_type,
        amount_unadjusted :: STRING AS amount_raw,
        amount_unadjusted :: FLOAT AS amount_raw_precise,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        nep_transfers
),
------------------------------   FINAL --------------------------------
transfer_union AS (
    SELECT
        *
    FROM
        nep_final
    UNION ALL
    SELECT
        *
    FROM
        native_final
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        rn,
        contract_address,
        from_address,
        to_address,
        memo,
        amount_raw,
        amount_raw_precise,
        transfer_type,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        transfer_union
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'action_id','contract_address','amount_raw','from_address','to_address','memo','rn']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
