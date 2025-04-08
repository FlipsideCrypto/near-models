{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::Date'],
    unique_key = 'mint_id',
    incremental_strategy = 'delete+insert',
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
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_base') }}
    {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}            
    {% elif is_incremental() %}
    WHERE modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
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
        f.value :amount :: variant AS amount_unadj,
        f.value :memo :: STRING AS memo,
        logs_rn + f.index AS rn,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        ft_mints
        JOIN LATERAL FLATTEN(
            input => DATA :data
        ) f
    WHERE
        amount_unadj > 0
)
SELECT  
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'action_id','contract_address','amount_unadj','from_address','to_address','memo','rn']
    )}} AS mint_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
    ft_mints_final
WHERE
    mint_id IS NOT NULL
