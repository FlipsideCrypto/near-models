{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_rainbow_id',
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,destination_address,source_address);",
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
    {% if var("MANUAL_FIX") %}
        WHERE {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
            WHERE modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
        {% endif %}
),

outbound_near_to_aurora AS (
    -- ft_transfer_call sends token to aurora
    -- EVM address logged in method action under msg
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        args :memo :: STRING AS memo,
        LPAD(
            IFF(len(SPLIT(args :msg :: STRING, ':') [1]) = 104, SUBSTR(args :msg :: STRING, -40), args :msg :: STRING),
            42,
            '0x'
        ) AS destination_address,
        signer_id AS source_address,
        'Aurora' AS destination_chain_id,
        'Near' AS source_chain_id,
        receipt_succeeded,
        method_name,
        'aurora' AS bridge_address,
        'outbound' AS direction,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        method_name = 'ft_transfer_call'
        AND args :receiver_id :: STRING = 'aurora'
        AND (
            receiver_id = 'aurora'
            OR receiver_id LIKE '%.factory.bridge.near'
        )
),

inbound_aurora_to_near AS (
    -- ft_transfer called on token contract, signed by relay.aurora
    -- recipient in actions JSON of ft_transfer, signer evm address in log of "submit" method
    -- no explicit mention of bridge method / contract
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
        'Near' AS destination_chain_id,
        'Aurora' AS source_chain_id,
        receipt_succeeded,
        method_name,
        'aurora' AS bridge_address,
        'inbound' AS direction,
        _partition_by_block_number,
        args
    FROM
        functioncall
    WHERE
        method_name = 'ft_transfer'
        AND signer_id = 'relay.aurora'
        AND NOT (
            -- Exclude 1 NEAR fee for fast bridge
            signer_id = 'relay.aurora'
            AND receiver_id = 'wrap.near'
            AND args :receiver_id :: STRING IN (
                '74abd625a1132b9b3258313a99828315b10ef864.aurora',
                '055707c67977e8217f98f19cfa8aca18b2282d0c.aurora',
                'e0302be5963b1f13003ab3a4798d2853bae731a7.aurora'
            )
        )
),

inbound_a2n_src_address AS (
    SELECT
        tx_hash,
        receipt_id,
        REGEXP_SUBSTR(
            clean_log,
            '0x[0-9a-fA-F]{40}'
        ) AS source_address
    FROM
        logs
    WHERE
        receiver_id = 'aurora'
        AND predecessor_id = 'relay.aurora'
        AND log_index = 0
        AND clean_log like 'signer_address%'
        AND
            tx_hash IN (
                SELECT
                    tx_hash
                FROM
                    inbound_aurora_to_near
            )
),

inbound_a2n_final AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_hash,
        A.receipt_id,
        A.action_index,
        A.token_address,
        A.amount_raw,
        A.memo,
        A.destination_address,
        b.source_address,
        A.destination_chain_id,
        A.source_chain_id,
        A.receipt_succeeded,
        A.method_name,
        A.bridge_address,
        A.direction,
        A._partition_by_block_number
    FROM
        inbound_aurora_to_near A
        LEFT JOIN inbound_a2n_src_address b
        ON A.tx_hash = b.tx_hash
),

outbound_near_to_eth AS (
    -- determined by finish_withdraw method call on factory.bridge.near
    -- if signed by aurora relayer, likely aurora<->eth bridge
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        signer_id = 'relay.aurora' AS is_aurora,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        NULL AS memo,
        LPAD(
            args :recipient :: STRING,
            42,
            '0x'
        ) AS destination_address,
        IFF(
            is_aurora,
            destination_address,
            signer_id
        ) AS source_address,
        'Ethereum' AS destination_chain_id,
        IFF(
            is_aurora,
            'Aurora',
            'Near'
        ) AS source_chain_id,
        receipt_succeeded,
        method_name,
        'factory.bridge.near' AS bridge_address,
        'outbound' AS direction,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                functioncall
            WHERE
                receiver_id = 'factory.bridge.near'
                AND method_name = 'finish_withdraw'
        )
        AND method_name = 'withdraw'
),
inbound_eth_to_near_txs AS (
    SELECT
        DISTINCT tx_hash
    FROM 
        functioncall
    WHERE
        receiver_id in ('factory.bridge.near', 'aurora')
        AND method_name = 'finish_deposit'
),
inbound_logs AS (
    SELECT
        receipt_id,
        ARRAY_AGG(clean_log) WITHIN GROUP (ORDER BY log_index) AS logs
    FROM
        logs
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                inbound_eth_to_near_txs
        )
    GROUP BY
        1
),
inbound_eth_to_near AS (
    -- determined by finish_deposit method call on factory.bridge.near
    SELECT
        fc.tx_hash,
        fc.receipt_id,
        block_id,
        block_timestamp,
        method_name,
        args,
        receiver_id,
        signer_id,
        logs,
        receipt_succeeded,
        _partition_by_block_number
    FROM
        functioncall fc
    LEFT JOIN inbound_logs l
        ON fc.receipt_id = l.receipt_id
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                inbound_eth_to_near_txs
        )
        AND method_name IN (
            'mint',
            'ft_transfer_call',
            'deposit',
            'finish_deposit'
        )
),
aggregate_inbound_eth_to_near_txs AS (
    SELECT
        tx_hash,
        MIN(block_id) AS block_id,
        MIN(block_timestamp) AS block_timestamp,
        OBJECT_AGG(
            method_name,
            OBJECT_CONSTRUCT(
                'args',
                args,
                'logs',
                logs,
                'receiver_id',
                receiver_id,
                'signer_id',
                signer_id
            )
        ) AS actions,
        booland_agg(receipt_succeeded) AS receipt_succeeded,
        MIN(_partition_by_block_number) AS _partition_by_block_number
    FROM
        inbound_eth_to_near
    GROUP BY
        1
),
inbound_e2n_final_ft AS (
    -- inbound token is minted on chain, take contract and amt from mint event
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        actions :ft_transfer_call :args :receiver_id :: STRING = 'aurora' AS is_aurora,
        actions :mint :receiver_id :: STRING AS token_address,
        actions :mint :args :amount :: INT AS amount_raw,
        actions :ft_transfer_call :args :memo :: STRING AS memo,
        LPAD(
            actions :ft_transfer_call :args :msg :: STRING,
            42,
            '0x'
        ) AS source_address,
        -- if minted by aurora contract, token is minted on near and bridged to aurora via xfer
        -- otherwise destination addr is the recipient of the transfer call
        IFF(
            is_aurora,
            source_address,
            COALESCE(
                actions :ft_transfer_call :args :receiver_id :: STRING,
                actions :mint :args :account_id :: STRING
            )
        ) AS destination_address,
        IFF(
            is_aurora,
            'Aurora',
            'Near'
        ) AS destination_chain_id,
        'Ethereum' AS source_chain_id,
        receipt_succeeded,
        IFF(
            is_aurora,
            'ft_transfer_call',
            'mint'
        ) AS method_name,
        'factory.bridge.near' AS bridge_address,
        'inbound' AS direction,
        _partition_by_block_number
    FROM
        aggregate_inbound_eth_to_near_txs
    WHERE
        actions :finish_deposit :receiver_id :: STRING != 'aurora'
),
inbound_e2n_final_eth AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        'aurora' AS token_address,
        COALESCE(
            REGEXP_SUBSTR(actions :finish_deposit :logs[1], 'Mint (\\d+) \\w+ tokens for: [a-fA-F0-9]+', 1, 1, 'e', 1),
            REGEXP_SUBSTR(actions :deposit :logs[1], 'NEP141Wei\\((\\d+)\\)', 1, 1, 'e', 1),
            REGEXP_SUBSTR(actions :finish_deposit :logs[2], 'Mint (\\d+) \\w+ tokens for: [a-fA-F0-9]+', 1, 1, 'e', 1),
            REGEXP_SUBSTR(actions :deposit :logs[1], 'with amount: (\\d+)', 1, 1, 'e', 1)
         ) AS amount_raw,
        COALESCE(
            REGEXP_SUBSTR(actions :finish_deposit :logs[1], 'for: ([^\\s]+)', 1, 1, 'e', 1),
            REGEXP_SUBSTR(actions :finish_deposit :logs[2], 'for: ([^\\s]+)', 1, 1, 'e', 1),
            REGEXP_SUBSTR(actions :deposit :logs[1], 'AccountId\\("([a-fA-F0-9]+)"\\)', 1, 1, 'e', 1),
            REGEXP_SUBSTR(actions :deposit :logs[1], 'to recipient \\"([^\\"]+)\\"', 1, 1, 'e', 1)
         ) AS destination_address,
        NULL as memo,
        CONCAT('0x', REGEXP_SUBSTR(actions :deposit :logs[1], 'from ([a-fA-F0-9]+)', 1, 1, 'e', 1)) AS source_address,
        'Ethereum' AS source_chain_id,
        'Near' AS destination_chain_id,
        receipt_succeeded,
        'mint' AS method_name,
        'prover.bridge.near' AS bridge_address,
        'inbound' AS direction,
        _partition_by_block_number
    FROM
        aggregate_inbound_eth_to_near_txs
    WHERE
        actions :finish_deposit :receiver_id :: STRING = 'aurora'
        
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
        bridge_address,
        direction,
        _partition_by_block_number
    FROM
        outbound_near_to_aurora
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
        bridge_address,
        direction,
        _partition_by_block_number
    FROM
        inbound_a2n_final
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
        bridge_address,
        direction,
        _partition_by_block_number
    FROM
        outbound_near_to_eth
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
        bridge_address,
        direction,
        _partition_by_block_number
    FROM
        inbound_e2n_final_ft
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
        bridge_address,
        direction,
        _partition_by_block_number
    FROM
        inbound_e2n_final_eth
)
SELECT
    *,
    amount_raw AS amount_adj,
    'rainbow' AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_hash', 'source_chain_id', 'destination_address', 'token_address', 'amount_raw']
    ) }} AS bridge_rainbow_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
