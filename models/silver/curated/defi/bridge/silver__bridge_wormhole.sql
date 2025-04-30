{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_wormhole_id',
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['curated','scheduled_non_core', 'grail'],
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
        AND (
            receipt_signer_id LIKE '%.portalbridge.near'
            OR receipt_receiver_id LIKE '%.portalbridge.near'
        )
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
logs AS (
    SELECT 
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        clean_log,
        log_index
    FROM {{ ref('silver__logs_s3') }}
    WHERE 
        tx_hash in (
            SELECT
                DISTINCT tx_hash
            FROM
                functioncall
        )
    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer') }}
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
outbound_near AS (
    -- burn
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        args :receiver :: STRING AS destination_address,
        signer_id AS source_address,
        args :chain :: INT AS destination_chain_id,
        15 AS source_chain_id,
        receipt_succeeded,
        method_name,
        'outbound' AS direction,
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
        receipt_id,
        action_index,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        args :account_id :: STRING AS destination_address,
        NULL AS source_address,
        args :recipient_chain :: INT AS destination_chain_id,
        receipt_succeeded,
        method_name,
        'inbound' AS direction,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        method_name = 'vaa_transfer'
),
inbound_src_id AS (
    SELECT
        tx_hash,
        receipt_id,
        REGEXP_SUBSTR(
            clean_log,
            '\\d+'
        ) :: INT AS wormhole_chain_id
    FROM
        logs
    WHERE
        receiver_id = 'contract.portalbridge.near'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                inbound_to_near
        )
        AND log_index = 1

),
inbound_final AS (
    SELECT
        block_id,
        block_timestamp,
        i.tx_hash,
        i.receipt_id,
        i.action_index,
        token_address,
        amount_raw,
        memo,
        destination_address,
        source_address,
        destination_chain_id,
        src.wormhole_chain_id AS source_chain_id,
        receipt_succeeded,
        method_name,
        direction,
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
        destination_chain_id,
        source_chain_id,
        receipt_succeeded,
        method_name,
        direction,
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
        destination_chain_id,
        source_chain_id,
        receipt_succeeded,
        method_name,
        direction,
        _partition_by_block_number
    FROM
        inbound_final
)
SELECT
    *,
    amount_raw AS amount_adj,
    'portalbridge.near' AS bridge_address,
    'wormhole' AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'destination_address', 'source_address']
    ) }} AS bridge_wormhole_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
