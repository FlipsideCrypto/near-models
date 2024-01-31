{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_allbridge_id',
    tags = ['curated'],
) }}

WITH functioncall AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receiver_id = 'bridge.a11bd.near' 
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter('_modified_timestamp') }}
        {% endif %}
),
outbound_near AS (
    -- burn
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
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
        _inserted_timestamp,
        _partition_by_block_number,
        modified_timestamp AS _modified_timestamp
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
        args :token_id :: STRING AS token_address,
        args :unlock_args :amount :: INT AS amount_raw,
        args :fee :: INT AS amount_fee_raw,
        args :memo :: STRING AS memo,
        args :unlock_args :recipient :: STRING AS destination_address,
        null AS source_address,
        'near' AS destination_chain_id,
        LOWER(
            args :unlock_args :lock_source :: STRING
        ) AS source_chain_id,
        args,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number,
        modified_timestamp AS _modified_timestamp
    FROM
        functioncall
    WHERE
        method_name = 'callback_create_unlock'
),
FINAL AS (
    SELECT
        *
    FROM
        outbound_near
    UNION ALL
    SELECT
        *
    FROM
        inbound_to_near
)
SELECT
    *,
    'allbridge' AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'token_address', 'amount_raw', 'source_chain_id', 'destination_address']
    ) }} AS bridge_allbridge_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
