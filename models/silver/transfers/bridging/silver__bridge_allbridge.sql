{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_allbridge_id',
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH functioncall AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receipt_predecessor_id AS predecessor_id,
        receipt_receiver_id AS receiver_id,
        action_data :method_name :: STRING AS method_name,
        action_data :args :: VARIANT AS args,
        receipt_succeeded,
        FLOOR(block_id, -3) AS _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        action_name = 'FunctionCall'
        AND receiver_id = 'bridge.a11bd.near'
        {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
        {% else %}
        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
        {% endif %}
),
metadata AS (
    SELECT
        contract_address,
        NAME,
        symbol,
        decimals
    FROM
        {{ ref('silver__ft_contract_metadata') }}
),
outbound_near AS (
    -- burn
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        args :create_lock_args :token_id :: STRING AS token_address,
        args :create_lock_args :amount :: INT AS amount_raw,
        args :fee :: INT AS amount_fee_raw,
        args :memo :: STRING AS memo,
        args :create_lock_args :recipient :: STRING AS destination_address,
        args :create_lock_args :sender :: STRING AS source_address,
        LOWER(
            args :create_lock_args :destination :: STRING
        ) AS destination_chain_id,
        'near' AS source_chain_id,
        args,
        receipt_succeeded,
        method_name,
        'outbound' AS direction,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        method_name = 'callback_create_lock'
),
inbound_to_near AS (
    -- mint
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        args :token_id :: STRING AS token_address,
        args :unlock_args :amount :: INT AS amount_raw,
        args :fee :: INT AS amount_fee_raw,
        args :memo :: STRING AS memo,
        args :unlock_args :recipient :: STRING AS destination_address,
        NULL AS source_address,
        'near' AS destination_chain_id,
        LOWER(
            args :unlock_args :lock_source :: STRING
        ) AS source_chain_id,
        args,
        receipt_succeeded,
        method_name,
        'inbound' AS direction,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        method_name = 'callback_create_unlock'
),
FINAL_UNION AS (
    SELECT
        *
    FROM
        outbound_near
    UNION ALL
    SELECT
        *
    FROM
        inbound_to_near
),
FINAL AS (
    SELECT
        *,
        GREATEST(amount_raw,
            RPAD(
                amount_raw,
                m.decimals,
                '0'
            )) :: NUMBER AS amount_adj,
        'bridge.a11bd.near' AS bridge_address,
        'allbridge' AS platform
    FROM
        FINAL_UNION
        JOIN metadata m ON 
        FINAL_UNION.token_address = m.contract_address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index']
    ) }} AS bridge_allbridge_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
