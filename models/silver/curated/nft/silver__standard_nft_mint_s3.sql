{{ config(
    materialized = "incremental",
    cluster_by = ["block_timestamp::DATE"],
    unique_key = "mint_action_id",
    incremental_strategy = "merge",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,tx_signer,tx_receiver,receipt_object_id,receiver_id,signer_id,owner_id,token_id);",
    tags = ['curated','scheduled_non_core']
) }}
{# Note - multisource model #}
WITH logs AS (

    SELECT
        log_id,
        receipt_object_id,
        tx_hash,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        gas_burnt,
        clean_log,
        is_standard,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__logs_s3') }}
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
tx AS (
    SELECT
        tx_hash,
        tx_signer,
        tx_receiver,
        tx_succeeded,
        tx_status, -- TODO deprecate col
        transaction_fee,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}
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
function_call AS (
    SELECT
        action_id,
        tx_hash,
        TRY_PARSE_JSON(args) AS args_json,
        method_name,
        deposit,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref("silver__actions_events_function_call_s3") }}
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
standard_logs AS (
    SELECT
        log_id AS logs_id,
        concat_ws(
            '-',
            receipt_object_id,
            '0'
        ) AS action_id,
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        gas_burnt,
        TRY_PARSE_JSON(clean_log) AS clean_log,
        COUNT(*) over (
            PARTITION BY tx_hash
        ) AS log_counter,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
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
        clean_log :data AS DATA,
        clean_log :event AS event,
        clean_log :standard AS STANDARD,
        clean_log :version AS version
    FROM
        standard_logs
        INNER JOIN function_call
        ON standard_logs.action_id = function_call.action_id
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
        _inserted_timestamp,
        _modified_timestamp
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
        _inserted_timestamp,
        _modified_timestamp
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
        tx_succeeded,
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
),
FINAL AS (
    SELECT
        mint_events.action_id,
        mint_events.mint_action_id,
        mint_events.tx_hash,
        mint_events.block_id,
        mint_events.block_timestamp,
        mint_events.method_name,
        mint_events.args_json AS args,
        mint_events.deposit,
        mint_tx.tx_signer AS tx_signer,
        mint_tx.tx_receiver AS tx_receiver,
        mint_tx.tx_succeeded AS tx_succeeded,
        mint_tx.tx_status AS tx_status,
        mint_events.receipt_object_id,
        mint_events.receiver_id,
        mint_events.signer_id,
        mint_events.owner_id,
        mint_events.token_id,
        mint_events.memo,
        mint_events.owner_per_tx,
        mint_events.mint_per_tx,
        mint_events.gas_burnt,
        -- gas burnt during receipt processing
        mint_tx.transaction_fee,
        -- gas burnt during entire transaction processing
        mint_events.log_counter,
        (
            mint_events.deposit / mint_events.log_counter
        ) :: FLOAT AS implied_price,
        mint_events._partition_by_block_number,
        mint_events._inserted_timestamp,
        mint_events._modified_timestamp
    FROM
        mint_events
        INNER JOIN mint_tx
        ON mint_events.tx_hash = mint_tx.tx_hash
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['mint_action_id']
    ) }} AS standard_nft_mint_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
