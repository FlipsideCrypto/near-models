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
        near.core.ez_actions
    WHERE 
        action_name = 'FunctionCall'
        AND (
            tx_receiver IN (SELECT contract_address FROM near_omni_contracts) 
            OR receipt_receiver_id IN (SELECT contract_address FROM near_omni_contracts)
            OR receipt_predecessor_id IN (SELECT contract_address FROM near_omni_contracts)
            OR action_data:args:receiver_id :: STRING IN (SELECT contract_address FROM near_omni_contracts)
        )
        -- AND tx_hash in ('7pU8WY4Mp1EuSeTXkyagKvVZBdxX3jqD2zXwZgGmYueA', 'AZAZTWJZytEJWu8nAhJBtLtFXkneQFCqXz4WdspziehB')
        -- AND block_timestamp >= CURRENT_DATE - INTERVAL '3 day'
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
        action_data,
        method_name,
        log_index,
        clean_log,
        receipt_id,
        logs_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id,
        _partition_by_block_number
    FROM
        actions
    JOIN logs 
        USING (block_id, tx_hash, receipt_id)
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
        TRY_PARSE_JSON(REGEXP_SUBSTR(clean_log :: STRING, '\\{.*\\}')) AS event_json,
        TRY_PARSE_JSON(REGEXP_SUBSTR(clean_log :: STRING, '\\{.*\\}')):"FinTransferEvent":"transfer_message" AS transfer_data,
        transfer_data:amount :: STRING AS amount_raw,
        SPLIT_PART(transfer_data:recipient :: STRING, ':', 1) AS destination_chain,
        SPLIT_PART(transfer_data:recipient :: STRING, ':', 2) AS destination_address,
        SPLIT_PART(transfer_data:sender :: STRING, ':', 1) AS source_chain,
        SPLIT_PART(transfer_data:sender :: STRING, ':', 2) AS source_address,
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
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                actions
            WHERE
                method_name = 'mint'
            GROUP BY 1
        )
        AND clean_log :: STRING LIKE '%FinTransferEvent%'
),
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
        TRY_PARSE_JSON(REGEXP_SUBSTR(clean_log :: STRING, '\\{.*\\}')) AS event_json,
        TRY_PARSE_JSON(REGEXP_SUBSTR(clean_log :: STRING, '\\{.*\\}')):"InitTransferEvent":"transfer_message" AS transfer_data,
        transfer_data:amount :: STRING AS amount_raw,
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
        tx_hash IN (
            SELECT DISTINCT 
                tx_hash
            FROM
                actions
            WHERE
                method_name = 'burn'
            GROUP BY 1
        )
        AND clean_log :: STRING LIKE '%InitTransferEvent%'
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
        direction,
        receipt_succeeded,
        method_name,
        bridge_address,
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
        direction,
        receipt_succeeded,
        method_name,
        bridge_address,
        _partition_by_block_number
    FROM
        outbound_omni
) 
select
    count(*),
    count(case when direction = 'inbound' then 1 end) as inbound_count,
    count(case when direction = 'outbound' then 1 end) as outbound_count,
    min(block_timestamp) as min_block_timestamp,
    max(block_timestamp) as max_block_timestamp,
    array_agg(distinct source_chain_id) as source_chain_ids,
    array_agg(distinct destination_chain_id) as destination_chain_ids,
    count(case when receipt_succeeded = false then 1 end) as failed_count,
    count(case when receipt_succeeded = true then 1 end) as succeeded_count
from final