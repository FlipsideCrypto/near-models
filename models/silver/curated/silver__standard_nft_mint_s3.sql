{{ config(
    materialized = "incremental",
    cluster_by = ["_load_timestamp::DATE","block_timestamp::DATE"],
    unique_key = "mint_action_id",
    incremental_strategy = "delete+insert",
    tags = ['curated']
) }}

WITH logs AS (

    SELECT
        *
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
tx AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
function_call AS (
    SELECT
        action_id,
        tx_hash,
        TRY_PARSE_JSON(args) AS args_json,
        method_name,
        deposit
    FROM
        {{ ref("silver__actions_events_function_call_s3") }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
standard_logs AS (
    SELECT
        action_id AS logs_id,
        concat_ws('-', receipt_object_id, '0') as action_id,
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        gas_burnt,
        _LOAD_TIMESTAMP,
        _PARTITION_BY_BLOCK_NUMBER,
        TRY_PARSE_JSON(clean_log) AS clean_log,
        COUNT(*) OVER (PARTITION BY tx_hash) AS log_counter
    FROM
        logs
    WHERE
        is_standard = TRUE
),
nft_events AS (
    SELECT
        standard_logs.*,
        function_call.method_name,
        function_call.deposit,
        function_call.args_json,
        clean_log :data AS DATA,
        clean_log :event AS event,
        clean_log :standard AS STANDARD,
        clean_log :version AS version
    FROM
        standard_logs
        LEFT JOIN function_call
    ON standard_logs.ACTION_ID = function_call.ACTION_ID
    WHERE
        STANDARD = 'nep171' -- nep171 nft STANDARD, version  nep245 IS multitoken STANDARD,  nep141 IS fungible token STANDARD
        AND event = 'nft_mint'
),
raw_mint_events AS (
    SELECT
        action_id,
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        gas_burnt,
        _LOAD_TIMESTAMP,
        _PARTITION_BY_BLOCK_NUMBER,
        INDEX AS batch_index,
        args_json,
        method_name,
        deposit,
        ARRAY_SIZE(
            DATA :: ARRAY
        ) AS owner_per_tx,
        VALUE :owner_id :: STRING AS owner_id,
        VALUE :token_ids :: ARRAY AS tokens,
        TRY_PARSE_JSON(
            VALUE :memo
        ) AS memo,
        log_counter
    FROM
        nft_events,
        LATERAL FLATTEN(
            input => DATA
        )
),
mint_events AS (
    SELECT
        action_id,
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        _LOAD_TIMESTAMP,
        _PARTITION_BY_BLOCK_NUMBER,
        args_json,
        method_name,
        deposit,
        owner_per_tx,
        gas_burnt,
        batch_index,
        owner_id,
        memo,
        INDEX AS token_index,
        ARRAY_SIZE(
            tokens
        ) AS mint_per_tx,
        VALUE :: STRING AS token_id,
        concat_ws(
            '-',
            action_id,
            COALESCE(batch_index, '0'),
            COALESCE(token_index, '0'),
            COALESCE(token_id, '0')
        ) AS mint_action_id,
        log_counter
    FROM
        raw_mint_events,
        LATERAL FLATTEN(
            input => tokens
        )
),
mint_tx AS (
    SELECT
        tx_hash,
        tx_signer,
        tx_receiver,
        tx_status,
        transaction_fee
    FROM
        tx
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                mint_events
        )
)

SELECT
    mint_events.action_id,
    mint_events.mint_action_id,
    mint_events.tx_hash,
    mint_events.block_id,
    mint_events.block_timestamp,
    mint_events.method_name,
    mint_events.args_json as args,
    mint_events.deposit,
    mint_tx.tx_signer AS tx_signer,
    mint_tx.tx_receiver AS tx_receiver,
    mint_tx.tx_status AS tx_status,
    mint_events.receipt_object_id,
    mint_events.receiver_id,
    mint_events.signer_id,
    mint_events.owner_id,
    mint_events.token_id,
    mint_events.memo,
    mint_events.owner_per_tx,
    mint_events.mint_per_tx,
    mint_events.gas_burnt, -- gas burnt during receipt processing
    mint_tx.transaction_fee, -- gas burnt during entire transaction processing
    mint_events._LOAD_TIMESTAMP,
    mint_events._PARTITION_BY_BLOCK_NUMBER,
    mint_events.log_counter,
    (mint_events.deposit / mint_events.log_counter) :: FLOAT as implied_price
FROM
    mint_events
    LEFT JOIN mint_tx
    ON mint_events.tx_hash = mint_tx.tx_hash