{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['fact_intents_id'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,token_id);",
    tags = ['intents','curated','scheduled_non_core']
) }}
-- incremental logic if execute block
WITH intent_txs AS (

    SELECT
        DISTINCT tx_hash
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        block_timestamp >= '2024-11-01'
        AND receiver_id = 'intents.near'
        AND receipt_actions :receipt :Action :actions [0] :FunctionCall :method_name :: STRING = 'execute_intents' -- incremental logic
),
nep245_logs AS (
    SELECT
        block_timestamp,
        block_id,
        l.tx_hash,
        receipt_object_id AS receipt_id,
        log_index,
        receiver_id,
        predecessor_id,
        signer_id,
        gas_burnt,
        TRY_PARSE_JSON(clean_log) :event :: STRING AS log_event,
        TRY_PARSE_JSON(clean_log) :data :: ARRAY AS log_data,
        ARRAY_SIZE(log_data) AS log_data_len,
        receipt_succeeded
    FROM
        {{ ref('silver__logs_s3') }}
        l
        JOIN intent_txs r
        ON r.tx_hash = l.tx_hash
    WHERE
        receiver_id = 'intents.near'
        AND block_timestamp >= '2024-11-01'
        AND TRY_PARSE_JSON(clean_log) :standard :: STRING = 'nep245' -- incremental logic
),
flatten_logs AS (
    SELECT
        l.block_timestamp,
        l.block_id,
        l.tx_hash,
        l.receipt_id,
        l.receiver_id,
        l.predecessor_id,
        l.log_event,
        l.gas_burnt,
        this AS log_event_this,
        INDEX AS log_event_index,
        VALUE :amounts :: ARRAY AS amounts,
        VALUE :token_ids :: ARRAY AS token_ids,
        VALUE :owner_id :: STRING AS owner_id,
        VALUE :old_owner_id :: STRING AS old_owner_id,
        VALUE :new_owner_id :: STRING AS new_owner_id,
        VALUE :memo :: STRING AS memo,
        l.log_index,
        l.receipt_succeeded,
        ARRAY_SIZE(amounts) AS amounts_size,
        ARRAY_SIZE(token_ids) AS token_ids_size
    FROM
        nep245_logs l,
        LATERAL FLATTEN(
            input => log_data
        )
),
flatten_arrays AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        log_index,
        log_event_index,
        owner_id,
        old_owner_id,
        new_owner_id,
        memo,
        INDEX AS amount_index,
        VALUE :: STRING AS amount_raw,
        token_ids [INDEX] AS token_id,
        gas_burnt,
        receipt_succeeded
    FROM
        flatten_logs,
        LATERAL FLATTEN(
            input => amounts
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'receipt_id', 'log_index', 'log_event_index', 'amount_index']
    ) }} AS fact_intents_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_arrays
