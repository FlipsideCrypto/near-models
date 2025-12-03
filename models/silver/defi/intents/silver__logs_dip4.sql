{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['dip4_id'],
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
        log_json :: VARIANT AS raw_log_json,
        log_json :event :: STRING AS log_event,
        log_json :version :: STRING AS dip4_version,
        log_json :data AS log_data,
        receipt_succeeded,
        modified_timestamp
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        receiver_id = 'intents.near'
        AND block_timestamp >= '2024-11-01'
        AND TRY_PARSE_JSON(clean_log) :standard :: STRING = 'dip4'
        AND TRY_PARSE_JSON(clean_log) :event :: STRING NOT IN (
            'storage_deposit',
            'public_key_added',
            'public_key_removed',
            'set_auth_by_predecessor_id'
        )

        {% if is_incremental() %}
            AND modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
),
-- Events with data as array containing simple fields
simple_array_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        INDEX AS log_event_index,
        -- Common fields
        VALUE :account_id :: STRING AS account_id,
        COALESCE(VALUE :intent_hash :: STRING, VALUE :hash :: STRING) AS intent_hash,
        -- Withdraw fields
        VALUE :receiver_id :: STRING AS withdraw_receiver_id,
        VALUE :amount :: STRING AS amount,
        VALUE :token :: STRING AS token,
        VALUE :token_id :: STRING AS token_id,
        -- Null placeholders for other event types
        NULL AS memo,
        NULL AS token_key,
        NULL AS token_value,
        NULL AS referral,
        0 AS array_index
    FROM
        logs_base,
        LATERAL FLATTEN(
            input => log_data
        )
    WHERE
        log_event IN ('intents_executed', 'ft_withdraw', 'native_withdraw', 'nft_withdraw')
),
-- mt_withdraw: has arrays (amounts, token_ids) that need flattening
mt_withdraw_flatten AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        INDEX AS log_event_index,
        VALUE :account_id :: STRING AS account_id,
        VALUE :intent_hash :: STRING AS intent_hash,
        VALUE :receiver_id :: STRING AS withdraw_receiver_id,
        VALUE :token :: STRING AS token,
        VALUE :memo :: STRING AS memo,
        VALUE :amounts :: ARRAY AS amounts,
        VALUE :token_ids :: ARRAY AS token_ids
    FROM
        logs_base,
        LATERAL FLATTEN(
            input => log_data
        )
    WHERE
        log_event = 'mt_withdraw'
),
mt_withdraw_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        log_event_index,
        account_id,
        intent_hash,
        withdraw_receiver_id,
        token,
        memo,
        INDEX AS array_index,
        VALUE :: STRING AS amount,
        token_ids[INDEX] :: STRING AS token_id,
        NULL AS token_key,
        NULL AS token_value,
        NULL AS referral
    FROM
        mt_withdraw_flatten,
        LATERAL FLATTEN(
            input => amounts
        )
),
-- token_diff: has diff{} and optional fees_collected{} objects to flatten
token_diff_base AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        INDEX AS log_event_index,
        VALUE :account_id :: STRING AS account_id,
        VALUE :intent_hash :: STRING AS intent_hash,
        VALUE :referral :: STRING AS referral,
        VALUE :diff :: OBJECT AS diff,
        VALUE :fees_collected :: OBJECT AS fees_collected
    FROM
        logs_base,
        LATERAL FLATTEN(
            input => log_data
        )
    WHERE
        log_event = 'token_diff'
),
token_diff_diff_flatten AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        log_event_index,
        account_id,
        intent_hash,
        referral,
        'diff' AS token_source,
        KEY :: STRING AS token_key,
        VALUE :: STRING AS token_value
    FROM
        token_diff_base,
        LATERAL FLATTEN(
            input => diff
        )
),
token_diff_fees_flatten AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        log_event_index,
        account_id,
        intent_hash,
        referral,
        'fees_collected' AS token_source,
        KEY :: STRING AS token_key,
        VALUE :: STRING AS token_value
    FROM
        token_diff_base,
        LATERAL FLATTEN(
            input => fees_collected
        )
    WHERE
        fees_collected IS NOT NULL
),
token_diff_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        log_event_index,
        account_id,
        intent_hash,
        referral,
        token_key,
        token_value,
        NULL AS withdraw_receiver_id,
        NULL AS amount,
        NULL AS token,
        NULL AS token_id,
        NULL AS memo,
        ROW_NUMBER() OVER (PARTITION BY tx_hash, receipt_id, log_index, log_event_index ORDER BY token_source, token_key) - 1 AS array_index
    FROM (
        SELECT * FROM token_diff_diff_flatten
        UNION ALL
        SELECT * FROM token_diff_fees_flatten
    )
),
-- transfer: has tokens{} object to flatten
transfer_base AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        INDEX AS log_event_index,
        VALUE :account_id :: STRING AS account_id,
        VALUE :intent_hash :: STRING AS intent_hash,
        VALUE :receiver_id :: STRING AS transfer_receiver_id,
        VALUE :memo :: STRING AS memo,
        VALUE :tokens :: OBJECT AS tokens
    FROM
        logs_base,
        LATERAL FLATTEN(
            input => log_data
        )
    WHERE
        log_event = 'transfer'
),
transfer_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        log_event_index,
        account_id,
        intent_hash,
        transfer_receiver_id AS withdraw_receiver_id,
        memo,
        KEY :: STRING AS token_key,
        VALUE :: STRING AS token_value,
        NULL AS amount,
        NULL AS token,
        NULL AS token_id,
        NULL AS referral,
        ROW_NUMBER() OVER (PARTITION BY tx_hash, receipt_id, log_index, log_event_index ORDER BY KEY) - 1 AS array_index
    FROM
        transfer_base,
        LATERAL FLATTEN(
            input => tokens
        )
),
-- fee events: data is object (not array)
fee_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        0 AS log_event_index,
        NULL AS account_id,
        NULL AS intent_hash,
        NULL AS withdraw_receiver_id,
        NULL AS memo,
        NULL AS token_key,
        NULL AS token_value,
        NULL AS amount,
        NULL AS token,
        NULL AS token_id,
        NULL AS referral,
        0 AS array_index,
        log_data :old_fee :: STRING AS old_fee,
        log_data :new_fee :: STRING AS new_fee,
        log_data :old_fee_collector :: STRING AS old_fee_collector,
        log_data :new_fee_collector :: STRING AS new_fee_collector
    FROM
        logs_base
    WHERE
        log_event IN ('fee_changed', 'fee_collector_changed')
),
-- Union all event types
all_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        log_event_index,
        account_id,
        intent_hash,
        withdraw_receiver_id,
        amount,
        token,
        token_id,
        memo,
        token_key,
        token_value,
        referral,
        array_index,
        NULL AS old_fee,
        NULL AS new_fee,
        NULL AS old_fee_collector,
        NULL AS new_fee_collector
    FROM simple_array_events

    UNION ALL

    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        log_event_index,
        account_id,
        intent_hash,
        withdraw_receiver_id,
        amount,
        token,
        token_id,
        memo,
        token_key,
        token_value,
        referral,
        array_index,
        NULL AS old_fee,
        NULL AS new_fee,
        NULL AS old_fee_collector,
        NULL AS new_fee_collector
    FROM mt_withdraw_events

    UNION ALL

    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        log_event_index,
        account_id,
        intent_hash,
        withdraw_receiver_id,
        amount,
        token,
        token_id,
        memo,
        token_key,
        token_value,
        referral,
        array_index,
        NULL AS old_fee,
        NULL AS new_fee,
        NULL AS old_fee_collector,
        NULL AS new_fee_collector
    FROM token_diff_events

    UNION ALL

    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        dip4_version,
        raw_log_json,
        log_index,
        gas_burnt,
        receipt_succeeded,
        log_event_index,
        account_id,
        intent_hash,
        withdraw_receiver_id,
        amount,
        token,
        token_id,
        memo,
        token_key,
        token_value,
        referral,
        array_index,
        NULL AS old_fee,
        NULL AS new_fee,
        NULL AS old_fee_collector,
        NULL AS new_fee_collector
    FROM transfer_events

    UNION ALL

    SELECT * FROM fee_events
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    dip4_version,
    raw_log_json,
    log_index,
    log_event_index,
    array_index,
    account_id,
    intent_hash,
    withdraw_receiver_id,
    amount,
    token,
    token_id,
    memo,
    token_key,
    token_value,
    referral,
    old_fee,
    new_fee,
    old_fee_collector,
    new_fee_collector,
    gas_burnt,
    receipt_succeeded,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'receipt_id', 'log_index', 'log_event_index', 'array_index']
    ) }} AS dip4_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_events
