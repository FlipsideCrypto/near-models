{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_omni_id',
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core'],
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"]
)}}

-- token id in fact intents since it was deployed by the omni bridge contract
-- OBJECT KEYS (JSON) on InitTransferEvent/FinTransferEvent
-- DISTINCT (Recipient U Sender).split(:)[0]

WITH near_omni_contracts AS (
    SELECT 
        contract_address
    FROM (
        VALUES
            ('omni.bridge.near'), -- Main Omni Bridge contract
            ('omni-provider.bridge.near'), -- Helper contract
            ('vaa-prover.bridge.near') -- Wormhole verification helper contract
    ) AS contracts(contract_address)
),
actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        tx_succeeded,
        tx_receiver,
        tx_signer,
        receipt_predecessor_id,
        receipt_receiver_id,
        receipt_succeeded,
        action_index,
        action_name,
        action_data,
        action_data:method_name :: STRING AS method_name,
        action_data:args :: STRING AS args,
        receipt_id,
        actions_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id,
        FLOOR(block_id, -3) AS _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE 
        action_name = 'FunctionCall'
        AND (
            tx_receiver IN (SELECT contract_address FROM near_omni_contracts) 
            OR receipt_receiver_id IN (SELECT contract_address FROM near_omni_contracts)
            OR receipt_predecessor_id IN (SELECT contract_address FROM near_omni_contracts)
            OR action_data:args:receiver_id :: STRING IN (SELECT contract_address FROM near_omni_contracts)
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
has_mint_burn AS (
    SELECT 
        tx_hash,
        MAX(CASE WHEN method_name = 'mint' THEN TRUE ELSE FALSE END) AS has_mint,
        MAX(CASE WHEN method_name = 'burn' THEN TRUE ELSE FALSE END) AS has_burn
    FROM 
        actions
    GROUP BY 
        1
),
logs AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id,
        predecessor_id,
        signer_id,
        is_standard,
        log_index,
        clean_log,
        receipt_succeeded,
        receipt_id,
        logs_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        near.silver.logs_s3
    {% if var("MANUAL_FIX") %}
    WHERE
        {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            WHERE
                modified_timestamp >= (
                    SELECT
                        MAX(modified_timestamp)
                    FROM
                        {{ this }}
                )
        {% endif %}
    {% endif %}
),
joined AS (
    SELECT
        block_id, 
        block_timestamp,
        tx_hash,
        tx_succeeded,
        tx_receiver,
        tx_signer,
        receipt_predecessor_id,
        receipt_receiver_id,
        receipt_succeeded,
        action_index,
        action_name,
        action_data :: STRING AS action_data,
        method_name,
        log_index,
        clean_log :: STRING AS clean_log,
        receipt_id,
        logs_id,
        inserted_timestamp,
        modified_timestamp,
        COALESCE(mb.has_mint, FALSE) AS has_mint,
        COALESCE(mb.has_burn, FALSE) AS has_burn,
        _invocation_id,
        _partition_by_block_number
    FROM
        actions
    JOIN logs 
        USING (block_id, tx_hash, receipt_id)
    LEFT JOIN has_mint_burn mb
        USING (tx_hash)
),
inbound_omni AS (
    -- determined by having fin_transfer (initialization), fin_transfer_callback (the actual bridge log) with a FinTransferEvent, and mint method calls. the first two interacts with omni.bridge.near, whereas the latter seems to be *chain*.bridge.near

    SELECT
        block_id, 
        block_timestamp,
        tx_hash,
        tx_receiver,
        tx_signer,
        receipt_predecessor_id,
        receipt_receiver_id AS bridge_address,
        action_index,
        action_data,
        method_name,
        receipt_id,
        clean_log,
        receipt_succeeded,
        has_mint,
        FALSE AS has_burn,
        TRY_PARSE_JSON(REGEXP_SUBSTR(clean_log :: STRING, '\\{.*\\}')) AS event_json,
        TRY_PARSE_JSON(REGEXP_SUBSTR(clean_log :: STRING, '\\{.*\\}')):"FinTransferEvent":"transfer_message" AS transfer_data,
        transfer_data:amount :: INT AS amount_raw,
        SPLIT_PART(transfer_data:recipient :: STRING, ':', 1) AS destination_chain,
        SPLIT_PART(transfer_data:recipient :: STRING, ':', 2) AS destination_address,
        SPLIT_PART(transfer_data:sender :: STRING, ':', 1) AS source_chain,
        SPLIT_PART(transfer_data:sender :: STRING, ':', 2) AS source_address,
        SPLIT_PART(transfer_data:token :: STRING, ':', 1) AS token_chain,
        SPLIT_PART(transfer_data:token :: STRING, ':', 2) AS token_address,
        transfer_data:origin_nonce::NUMBER AS origin_nonce,
        transfer_data:destination_nonce::NUMBER AS destination_nonce,
        transfer_data:msg :: STRING AS memo,
        'inbound' AS direction,
        inserted_timestamp,
        _partition_by_block_number
    FROM
        joined
    WHERE
        clean_log :: STRING LIKE '%FinTransferEvent%'
) ,
outbound_omni AS (
    -- determined by having a sequence of action: ft_on_transfer, burn, and ft_resolve_transfer in that order. an outbound tx must have at least a burn associated with it.

    SELECT
        block_id, 
        block_timestamp,
        tx_hash,
        tx_receiver,
        tx_signer,
        receipt_predecessor_id,
        receipt_receiver_id AS bridge_address,
        action_index,
        action_data,
        method_name,
        receipt_id,
        clean_log,
        receipt_succeeded,
        FALSE AS has_mint,
        has_burn,
        TRY_PARSE_JSON(REGEXP_SUBSTR(clean_log :: STRING, '\\{.*\\}')) AS event_json,
        TRY_PARSE_JSON(REGEXP_SUBSTR(clean_log :: STRING, '\\{.*\\}')):"InitTransferEvent":"transfer_message" AS transfer_data,
        transfer_data:amount :: INT AS amount_raw,
        SPLIT_PART(transfer_data:recipient :: STRING, ':', 1) AS destination_chain,
        SPLIT_PART(transfer_data:recipient :: STRING, ':', 2) AS destination_address,
        SPLIT_PART(transfer_data:sender :: STRING, ':', 1) AS source_chain,
        SPLIT_PART(transfer_data:sender :: STRING, ':', 2) AS source_address,
        SPLIT_PART(transfer_data:token :: STRING, ':', 1) AS token_chain,
        SPLIT_PART(transfer_data:token :: STRING, ':', 2) AS token_address,
        transfer_data:origin_nonce::NUMBER AS origin_nonce,
        transfer_data:destination_nonce::NUMBER AS destination_nonce,
        transfer_data:msg :: STRING AS memo,
        'outbound' AS direction,
        inserted_timestamp,
        _partition_by_block_number
    FROM
        joined
    WHERE
        clean_log :: STRING LIKE '%InitTransferEvent%'
), 
final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        memo,
        destination_address,
        source_address,
        destination_chain as destination_chain_id,
        source_chain as source_chain_id,
        token_chain as token_chain_id,
        direction,
        receipt_succeeded,
        method_name,
        bridge_address,
        has_mint,
        has_burn,
        _partition_by_block_number
    FROM
        inbound_omni
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
        destination_chain as destination_chain_id,
        source_chain as source_chain_id,
        token_chain as token_chain_id,
        direction,
        receipt_succeeded,
        method_name,
        bridge_address,
        has_mint,
        has_burn,
        _partition_by_block_number
    FROM
        outbound_omni
) 
SELECT
    *,
    amount_raw AS amount_adj,
    'omni' AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'source_chain_id', 'destination_address', 'token_address', 'amount_raw']
    ) }} AS bridge_omni_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    final