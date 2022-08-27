{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
),
usn_tx AS (
    SELECT
        block_id,
        tx_hash,
        block_timestamp,
        tx_receiver,
        tx_signer,
        tx_status,
        tx,
        tx: actions[0]: FunctionCall: method_name :: STRING AS method_names,
        _ingested_at,
        _inserted_timestamp
    FROM
        txs
    WHERE
        tx_receiver = 'usn'
        OR tx_signer = 'usn'
),

transfer_call_withdraw_events AS (
    SELECT
        block_id,
        tx_hash,
        block_timestamp,
        tx_receiver,
        tx_signer,
        tx_status,
        tx,
        method_names,
        tx: receipt[0]: outcome: logs :: VARIANT AS events,
        _ingested_at,
        _inserted_timestamp
    FROM
        usn_tx
    WHERE
        method_names = 'ft_transfer_call'
        OR method_names = 'ft_transfer'
        OR method_names = 'withdraw'
),
buy_sell_events AS (
    SELECT
        block_id,
        tx_hash,
        block_timestamp,
        tx_receiver,
        tx_signer,
        tx_status,
        method_names,
        tx,
        tx: receipt[3]: outcome: logs :: VARIANT AS events,
        _ingested_at,
        _inserted_timestamp
    FROM
        usn_tx
    WHERE
        method_names = 'buy'
        OR method_names = 'sell'
),
parse_buy_sell_event AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        tx_status,
        method_names,
        flatten_events.value AS events,
        _ingested_at,
        _inserted_timestamp
    FROM
        buy_sell_events,
        LATERAL FLATTEN(
            input => buy_sell_events.events
        ) AS flatten_events
),
parse_transfer_withdraw_event AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        tx_status,
        method_names,
        flatten_events.value AS events,
        _ingested_at,
        _inserted_timestamp
    FROM
        transfer_call_withdraw_events,
        LATERAL FLATTEN(
            input => transfer_call_withdraw_events.events
        ) AS flatten_events
),
combined AS (
    SELECT
        *
    FROM
        parse_buy_sell_event
    UNION
    SELECT
        *
    FROM
        parse_transfer_withdraw_event
),
extract_events_logs AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        tx_status,
        method_names,
        SUBSTR(
            events,
            12,
            1000
        ) AS event_string,
        _ingested_at,
        _inserted_timestamp
    FROM
        combined
),
extract_logs_data AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        tx_status,
        method_names,
        PARSE_JSON(event_string) AS event_data,
        _ingested_at,
        _inserted_timestamp
    FROM
        extract_events_logs
),
FINAL AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        tx_status,
        method_names,
        event_data: data[0]: amount :: DOUBLE / pow(
            10,
            18
        ) AS amount,
        _ingested_at,
        _inserted_timestamp
    FROM
        extract_logs_data
)
SELECT
    *
FROM
    FINAL
