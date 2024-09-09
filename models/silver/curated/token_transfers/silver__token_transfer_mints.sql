{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_modified_timestamp::Date'],
    unique_key = 'mint_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}

WITH actions_events AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        signer_id,
        receiver_id,
        action_name,
        method_name,
        deposit,
        logs,
        receipt_succeeded,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_base') }}
    {% if is_incremental() %}
    WHERE _modified_timestamp >= (
        SELECT
            MAX(_modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
)
,
ft_mints AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        TRY_PARSE_JSON(REPLACE(VALUE, 'EVENT_JSON:')) AS DATA,
        b.index AS logs_rn,
        receiver_id AS contract_address,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        actions_events
        JOIN LATERAL FLATTEN(
            input => logs
        ) b
    WHERE
        DATA :event :: STRING IN (
            'ft_mint'
        )
),
ft_mints_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        contract_address,
        NVL(
            f.value :old_owner_id,
            NULL
        ) :: STRING AS from_address,
        NVL(
            f.value :new_owner_id,
            f.value :owner_id
        ) :: STRING AS to_address,
        f.value :amount :: variant AS amount_unadjusted,
        f.value :memo :: STRING AS memo,
        logs_rn + f.index AS rn,
        _inserted_timestamp,
        _modified_timestamp,
        _partition_by_block_number
    FROM
        ft_mints
        JOIN LATERAL FLATTEN(
            input => DATA :data
        ) f
    WHERE
        amount_unadjusted > 0
)
SELECT  
     {{ dbt_utils.generate_surrogate_key(
        ['action_id','rn']
    ) }} AS mint_id,
    *
FROM
    ft_mints_final