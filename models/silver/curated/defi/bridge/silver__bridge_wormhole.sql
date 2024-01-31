{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_wormhole_id',
    tags = ['curated'],
) }}

WITH functioncall AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        signer_id LIKE '%.portalbridge.near'
        OR receiver_id LIKE '%.portalbridge.near' 
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter('modified_timestamp') }}
        {% endif %}
),
outbound_near AS (
    -- burn
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id AS token_address,
        logs,
        args,
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        args :receiver :: STRING AS destination_address,
        signer_id AS source_address,
        'wormhole' AS bridge,
        args :chain :: INT AS destination_chain_id,
        15 AS source_chain_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        method_name = 'vaa_withdraw' 
        -- all the burns or withdraws are followed by a  publish_message in contract contract.w...to.near with the result
        -- example D35BNkK4gfPuuoWMGGJ6RNA3rjDoK66gPYASmfRy7rER (near)
        -- we can make sure that this is happening by checking that publish_message exists and is successful
),
inbound_to_near AS (
    -- mint
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id AS token_address,
        logs,
        args,
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        args :account_id :: STRING AS destination_address,
        NULL AS source_address,
        -- "In eth is Weth contract -- jum"
        'wormhole' AS bridge,
        args: recipient_chain :: INT AS destination_chain_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        method_name = 'vaa_transfer'
),
inbound_src_id AS (
    SELECT
        tx_hash,
        REGEXP_SUBSTR(
            logs [1],
            '\\d+'
        ) :: INT AS wormhole_chain_id
    FROM
        functioncall
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                inbound_to_near
        )
        AND method_name = 'submit_vaa'
        AND receiver_id = 'contract.portalbridge.near'
),
inbound_final AS (
    SELECT
        block_id,
        block_timestamp,
        i.tx_hash,
        token_address,
        logs,
        args,
        amount_raw,
        memo,
        destination_address,
        source_address,
        bridge,
        destination_chain_id,
        src.wormhole_chain_id AS source_chain_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        inbound_to_near i
        LEFT JOIN inbound_src_id src
        ON i.tx_hash = src.tx_hash
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        memo,
        destination_address,
        source_address,
        bridge,
        destination_chain_id,
        source_chain_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        outbound_near
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        memo,
        destination_address,
        source_address,
        bridge,
        destination_chain_id,
        source_chain_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        inbound_final
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'token_address', 'amount_raw', 'source_chain_id', 'destination_address']
    ) }} AS bridge_wormhole_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
