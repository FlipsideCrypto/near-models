{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'wormhole_rainbow_id',
    tags = ['curated'],
) }}

WITH functioncall AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        TRUE {% if var("MANUAL_FIX") %}
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
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        args :receiver :: STRING AS destination_address,
        signer_id AS source_address,
        'wormhole' AS bridge,
        args :chain :: INT AS destination_chain,
        -- map the contract to the chain  // 1.contract.portalbridge.near is weth
        'near' AS source_chain,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        method_name = 'vaa_withdraw' -- all the burns or withdraws are followed by a  publish_message in contract contract.w...to.near with the result
        -- example D35BNkK4gfPuuoWMGGJ6RNA3rjDoK66gPYASmfRy7rER (near)
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
        '----' AS source_address,
        -- "In eth is Weth contract -- jum"
        'wormhole' AS bridge,
        'near' AS destination_chain,
        -- `args :: receipient_chain` 15 = near
        receiver_id AS source_chain,
        -- map the contract to the chain
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        method_name = 'vaa_transfer'
)
SELECT
    *
FROM
    inbound_to_near
LIMIT
    10
