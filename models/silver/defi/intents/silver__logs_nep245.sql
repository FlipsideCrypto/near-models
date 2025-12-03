{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['nep245_id'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_non_core', 'intents']
) }}

WITH
logs_base AS(
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        log_index,
        receiver_id,
        predecessor_id,
        signer_id,
        gas_burnt,
        TRY_PARSE_JSON(clean_log) AS log_json,
        log_json :event :: STRING AS log_event,
        log_json :version :: STRING AS nep245_version,
        log_json :data :: ARRAY AS log_data,
        ARRAY_SIZE(log_data) AS log_data_len,
        receipt_succeeded,
        modified_timestamp
    FROM 
        {{ ref('silver__logs_s3') }}
    WHERE 
        receiver_id = 'intents.near'
        AND block_timestamp >= '2024-11-01'
        AND TRY_PARSE_JSON(clean_log) :standard :: STRING = 'nep245'

        {% if is_incremental() %}
            AND modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
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
        l.nep245_version,
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
        logs_base l,
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
        nep245_version,
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
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    nep245_version,
    log_index,
    log_event_index,
    owner_id,
    old_owner_id,
    new_owner_id,
    memo,
    amount_index,
    amount_raw,
    token_id,
    gas_burnt,
    receipt_succeeded,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'receipt_id', 'log_index', 'log_event_index']
    )}} AS nep245_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_arrays
