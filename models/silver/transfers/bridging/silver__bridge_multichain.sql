{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_multichain_id',
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core', 'unscheduled'],
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
        receipt_signer_id AS signer_id,
        action_data :method_name :: STRING AS method_name,
        action_data :args :: VARIANT AS args,
        receipt_succeeded,
        FLOOR(block_id, -3) AS _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        action_name = 'FunctionCall'
        AND action_data :method_name :: STRING = 'ft_transfer'  -- Both directions utilize ft_transfer
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
inbound AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        args :receiver_id :: STRING AS destination_address,
        NULL AS source_address,
        '1001313161554' AS destination_chain_id,
        SPLIT(
            memo,
            ':'
        ) [2] :: STRING AS source_chain_id,
        receipt_succeeded,
        method_name,
        'inbound' AS direction,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        signer_id = 'mpc-multichain.near'
),
outbound AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        SPLIT(
            memo,
            ' '
        ) [0] :: STRING AS destination_address,
        signer_id AS source_address,
        SPLIT(
            memo,
            ' '
        ) [1] :: STRING AS destination_chain_id,
        '1001313161554' AS source_chain_id,
        receipt_succeeded,
        method_name,
        'outbound' AS direction,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        args :receiver_id :: STRING = 'mpc-multichain.near'
),
FINAL AS (
    SELECT
        *
    FROM
        inbound
    UNION ALL
    SELECT
        *
    FROM
        outbound
)
SELECT
    *,
    amount_raw AS amount_adj,
    'mpc-multichain.near' AS bridge_address,
    'multichain' AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index']
    ) }} AS bridge_multichain_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
