{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_multichain_id',
    tags = ['curated', 'exclude_from_schedule'],
) }}

WITH functioncall AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        method_name = 'ft_transfer'  -- Both directions utilize ft_transfer
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter('_modified_timestamp') }}
        {% endif %}
),
inbound AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
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
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
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
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
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
    'multichain' AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'token_address', 'amount_raw', 'source_chain_id', 'destination_address']
    ) }} AS bridge_multichain_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
