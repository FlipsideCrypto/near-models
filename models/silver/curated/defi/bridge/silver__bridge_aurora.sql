{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'bridge_aurora_id',
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
            AND {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
),
aurora_functioncall AS (
    SELECT
        *
    FROM
        functioncall
    WHERE
        (
            (
                receiver_id IN (
                    'aurora',
                    'relay.aurora',
                    'factory.bridge.near'
                )
                OR receiver_id LIKE '%.factory.bridge.near'
            )
            OR signer_id IN (
                'aurora',
                'relay.aurora',
                'factory.bridge.near'
            )
        )
        AND method_name IN (
            'deposit',
            'finish_deposit',
            'finish_withdraw',
            'ft_on_transfer',
            'ft_resolve_transfer',
            'ft_transfer',
            'ft_transfer_call',
            'submit',
            'withdraw'
        )
),
outbound_near_to_aurora AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id AS token_address,
        args :amount :: STRING AS amount_raw,
        args :memo :: STRING AS memo,
        LPAD(
            IFF(len(SPLIT(args :msg :: STRING, ':') [1]) = 104, SUBSTR(args :msg :: STRING, -40), args :msg :: STRING),
            42,
            '0x'
        ) AS destination_address,
        signer_id AS source_address,
        'rainbow' AS bridge,
        'aurora' AS destination_chain,
        'near' AS source_chain,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        aurora_functioncall
    WHERE
        method_name = 'ft_transfer_call'
        AND args :receiver_id :: STRING = 'aurora'
),
inbound_aurora_to_near AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id AS token_address,
        args :amount :: STRING AS amount_raw,
        args :memo :: STRING AS memo,
        args :receiver_id :: STRING AS destination_address,
        'rainbow' AS bridge,
        'near' AS destination_chain,
        'aurora' AS source_chain,
        _inserted_timestamp,
        _partition_by_block_number,
        args
    FROM
        aurora_functioncall
    WHERE
        method_name = 'ft_transfer' -- AND signer_id
),
inbound_a2n_src_address AS (
    SELECT
        tx_hash,
        SUBSTRING(
            REGEXP_SUBSTR(
                logs [0] :: STRING,
                '\\(0x[0-9a-fA-F]{40}\\)'
            ),
            2,
            42
        ) AS source_address
    FROM
        aurora_functioncall
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                inbound_aurora_to_near
        ) -- AND some method / signer filter
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
        A.bridge,
        A.destination_chain,
        A.source_chain,
        A._inserted_timestamp,
        A._partition_by_block_number
    FROM
        inbound_aurora_to_near A
        LEFT JOIN inbound_a2n_src_address b
        ON A.tx_hash = b.tx_hash
),
outbound_near_to_eth AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id AS token_address,
        args :amount :: STRING AS amount_raw,
        NULL AS memo,
        LPAD(
            args :recipient :: STRING,
            42,
            '0x'
        ) AS destination_address,
        signer_id AS source_address,
        'rainbow' AS bridge,
        'ethereum' AS destination_chain,
        'near' AS source_chain,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        functioncall
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                aurora_functioncall
            WHERE
                receiver_id = 'factory.bridge.near'
                AND method_name = 'finish_withdraw'
        )
        AND method_name = 'withdraw'
),
inbound_eth_to_near AS (
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
        MIN(_inserted_timestamp) AS _inserted_timestamp,
        MIN(_partition_by_block_number) AS _partition_by_block_number
    FROM
        functioncall
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                aurora_functioncall
            WHERE
                receiver_id = 'factory.bridge.near'
                AND method_name = 'finish_deposit'
        )
        AND method_name IN (
            'mint',
            'ft_transfer_call'
        )
    GROUP BY
        1
),
inbound_e2n_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        actions :mint :receiver_id :: STRING AS token_address,
        actions :mint :args :amount :: STRING AS amount_raw,
        actions :ft_transfer_call :args :memo :: STRING AS memo,
        LPAD(
            actions :ft_transfer_call :args :msg :: STRING,
            42,
            '0x'
        ) AS source_address,
        IFF(
            actions :mint :args :account_id :: STRING = 'aurora',
            source_address,
            actions :ft_transfer_call :args :receiver_id :: STRING
        ) AS destination_address,
        'rainbow' AS bridge,
        IFF(
            actions :mint :args :account_id :: STRING = 'aurora',
            'aurora',
            'near'
        ) AS destination_chain,
        'ethereum' AS source_chain,
        _inserted_timestamp,
        _partition_by_block_number
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
        bridge,
        destination_chain,
        source_chain,
        _inserted_timestamp,
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
        bridge,
        destination_chain,
        source_chain,
        _inserted_timestamp,
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
        bridge,
        destination_chain,
        source_chain,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        outbound_near_to_eth
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address AS token_contract, -- NOTE we use token_contract in prices but xchain bridging uses token_address
        amount_raw,
        memo,
        destination_address,
        source_address,
        bridge,
        destination_chain, -- TODO use ID instead of chain str?
        source_chain,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        inbound_e2n_final
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'token_address', 'amount_raw', 'destination_address']
    ) }} AS bridge_aurora_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
