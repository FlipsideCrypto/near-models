{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_rainbow_id',
    cluster_by = ['block_timestamp::DATE', '_modified_timestamp::DATE'],
    tags = ['curated'],
) }}

WITH functioncall AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        method_name,
        args,
        logs,
        receiver_id,
        signer_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}

        {% if var("MANUAL_FIX") %}
            WHERE {{ partition_load_manual('no_buffer') }}
        {% else %}

            {% if is_incremental() %}
            WHERE _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
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
        _inserted_timestamp,
        _partition_by_block_number,
        _modified_timestamp
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
        _inserted_timestamp,
        _partition_by_block_number,
        args,
        _modified_timestamp
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
        REGEXP_SUBSTR(
            logs [0] :: STRING,
            '0x[0-9a-fA-F]{40}'
        ) AS source_address
    FROM
        functioncall
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                inbound_aurora_to_near
        )
        AND method_name = 'submit'
),
inbound_a2n_final AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_hash,
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
        A._inserted_timestamp,
        A._partition_by_block_number,
        A._modified_timestamp
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
        _inserted_timestamp,
        _partition_by_block_number,
        _modified_timestamp
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
inbound_eth_to_near AS (
    -- determined by finish_deposit method call on factory.bridge.near
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
        MIN(_inserted_timestamp) AS _inserted_timestamp,
        MIN(_partition_by_block_number) AS _partition_by_block_number,
        MIN(_modified_timestamp) AS _modified_timestamp
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
                AND method_name = 'finish_deposit'
        )
        AND method_name IN (
            'mint',
            'ft_transfer_call',
            'finish_deposit'
        )
    GROUP BY
        1
),
inbound_e2n_final AS (
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
        _inserted_timestamp,
        _partition_by_block_number,
        _modified_timestamp
    FROM
        inbound_eth_to_near
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
        _inserted_timestamp,
        _partition_by_block_number,
        _modified_timestamp
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
        _inserted_timestamp,
        _partition_by_block_number,
        _modified_timestamp
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
        _inserted_timestamp,
        _partition_by_block_number,
        _modified_timestamp
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
        _inserted_timestamp,
        _partition_by_block_number,
        _modified_timestamp
    FROM
        inbound_e2n_final
)
SELECT
    *,
    'rainbow' AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'source_chain_id', 'destination_address']
    ) }} AS bridge_rainbow_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
