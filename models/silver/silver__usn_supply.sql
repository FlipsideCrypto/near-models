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
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        tx: receipt [3]: outcome: logs :: variant AS events,
        tx: actions [0]: FunctionCall: method_name :: STRING AS method_names,
        _ingested_at,
        _inserted_timestamp,
        tx
    FROM
        transactions
    WHERE
        tx_receiver = 'usn'
        OR tx_signer = 'usn'
),
parse_event AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        method_names,
        flatten_events.value AS events,
        _ingested_at,
        _inserted_timestamp
    FROM
        usn_tx,
        LATERAL FLATTEN(
            input => usn_tx.events
        ) AS flatten_events
),
extract_events_logs AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        method_names,
        SUBSTR(
            events,
            12,
            1000
        ) AS event_string,
        _ingested_at,
        _inserted_timestamp
    FROM
        parse_event
),
extract_logs_data AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        method_names,
        PARSE_JSON(event_string) AS event_data,
        _ingested_at,
        _inserted_timestamp
    FROM
        extract_events_logs
),
extract_event_data AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        method_names,
        event_data: data [0]: amount :: DOUBLE / pow(
            10,
            18
        ) AS amount,
        _ingested_at,
        _inserted_timestamp
    FROM
        extract_logs_data
),
buy_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        method_names,
        amount,
        _ingested_at,
        _inserted_timestamp
    FROM
        extract_event_data
    WHERE
        method_names = 'buy'
),
sell_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        tx_receiver,
        tx_signer,
        method_names,
        amount,
        _ingested_at,
        _inserted_timestamp
    FROM
        extract_event_data
    WHERE
        method_names = 'sell'
),
FINAL AS (
    SELECT
        *
    FROM
        buy_events
    UNION
    SELECT
        *
    FROM
        sell_events
)
SELECT
    *
FROM
    FINAL
