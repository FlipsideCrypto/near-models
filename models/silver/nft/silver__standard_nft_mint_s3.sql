{{ config(
    materialized = "incremental",
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    cluster_by = ["block_timestamp::DATE"],
    unique_key = "mint_action_id",
    incremental_strategy = "merge",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,tx_signer,tx_receiver,receipt_object_id,receiver_id,signer_id,owner_id,token_id);",
    tags = ['curated','scheduled_non_core']
) }}

WITH logs AS (

    SELECT
        log_id,
        receipt_id,
        tx_hash,
        block_id,
        block_timestamp,
        predecessor_id,
        receiver_id,
        signer_id,
        gas_burnt,
        clean_log,
        is_standard,
        _partition_by_block_number
    FROM
        {{ ref('silver__logs_s3') }}
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
function_call AS (
    SELECT
        tx_hash,
        receipt_id,
        action_data :method_name :: STRING AS method_name,
        action_data :args :: STRING AS args_json,
        action_data :deposit :: FLOAT AS deposit,
        receipt_succeeded,
        tx_succeeded,
        tx_receiver,
        tx_signer,
        tx_fee,
        action_index
    FROM
        {{ ref("core__ez_actions") }}
    WHERE
        action_name = 'FunctionCall'
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
standard_logs AS (
    SELECT
        log_id AS logs_id,
        receipt_id,
        tx_hash,
        block_id,
        block_timestamp,
        predecessor_id,
        receiver_id,
        signer_id,
        gas_burnt,
        TRY_PARSE_JSON(clean_log) AS clean_log,
        COUNT(*) over (
            PARTITION BY tx_hash
        ) AS log_counter,
        _partition_by_block_number
    FROM
        logs
    WHERE
        is_standard
),
nft_events AS (
    SELECT
        standard_logs.*,
        function_call.method_name,
        function_call.deposit,
        function_call.args_json,
        clean_log :data :: VARIANT AS DATA,
        clean_log :event :: VARIANT AS event,
        clean_log :standard :: STRING AS STANDARD,
        clean_log :version :: STRING AS version,
        function_call.receipt_succeeded,
        function_call.tx_succeeded,
        function_call.tx_receiver,
        function_call.tx_signer,
        function_call.tx_fee,
        function_call.action_index
    FROM
        standard_logs
        INNER JOIN function_call
        ON standard_logs.receipt_id = function_call.receipt_id
        AND function_call.action_index = 0
    WHERE
        STANDARD = 'nep171' -- nep171 nft STANDARD, version  nep245 IS multitoken STANDARD,  nep141 IS fungible token STANDARD
        AND event = 'nft_mint'
),
raw_mint_events AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        predecessor_id,
        receiver_id,
        signer_id,
        gas_burnt,
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
        log_counter,
        _partition_by_block_number,
        receipt_succeeded,
        tx_succeeded,
        tx_receiver,
        tx_signer,
        tx_fee
    FROM
        nft_events,
        LATERAL FLATTEN(
            input => DATA
        )
),
mint_events AS (
    SELECT
        tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        predecessor_id,
        receiver_id,
        signer_id,
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
            receipt_id || '-' || '0',
            COALESCE(
                batch_index,
                '0'
            ),
            COALESCE(
                token_index,
                '0'
            ),
            COALESCE(
                token_id,
                '0'
            )
        ) AS mint_action_id,
        log_counter,
        _partition_by_block_number,
        receipt_succeeded,
        tx_succeeded,
        tx_receiver,
        tx_signer,
        tx_fee
    FROM
        raw_mint_events,
        LATERAL FLATTEN(
            input => tokens
        )
)
SELECT
    tx_hash,
    receipt_id AS receipt_object_id, -- for cutover
    receipt_id,
    block_id,
    block_timestamp,
    tx_receiver,
    tx_signer,
    predecessor_id,
    receiver_id,
    signer_id,
    TRY_PARSE_JSON(args_json) AS args,
    method_name,
    deposit,
    owner_per_tx,
    gas_burnt,
    batch_index,
    owner_id,
    memo,
    token_index,
    mint_per_tx,
    token_id,
    mint_action_id,
    log_counter,
    (
        deposit / log_counter
    ) :: FLOAT AS implied_price,
    tx_fee AS transaction_fee,
    tx_succeeded,
    receipt_succeeded,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['mint_action_id']
    ) }} AS standard_nft_mint_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    mint_events
