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
    tx: actions[0]: FunctionCall: method_name :: STRING AS method_names,
    tx: receipt[0] :: VARIANT AS receipts,
    tx: receipt[0]: outcome: logs :: VARIANT AS events,
    _ingested_at,
    _inserted_timestamp
FROM txs
WHERE tx_receiver = 'usn'
),

usn_method_call AS (
SELECT 
    block_id,
    tx_hash,
    block_timestamp,
    tx_receiver,
    tx_signer,
    method_names,
    events,
    _ingested_at,
    _inserted_timestamp
FROM usn_tx
WHERE method_names='ft_transfer_call' or method_names='ft_transfer' or method_names='withdraw'
),

parse_event AS (
SELECT
    block_id,
    tx_hash,
    block_timestamp,
    tx_receiver,
    tx_signer,
    method_names,
    flatten_events.value AS events,
    _ingested_at,
    _inserted_timestamp
FROM usn_method_call,
lateral flatten(input => usn_method_call.events) AS flatten_events
),

extract_events AS (
SELECT
    block_id,
    tx_hash,
    block_timestamp,
    tx_receiver,
    tx_signer,
    method_names,
    substr(events, 12, 1000) AS event_string,
    _ingested_at,
    _inserted_timestamp
FROM parse_event
),

extract_data AS (
SELECT 
    method_names,
    block_id,
    tx_hash,
    block_timestamp,
    tx_receiver,
    tx_signer,
    parse_json(event_string) AS event_data,
    _ingested_at,
    _inserted_timestamp
FROM extract_events
),

final AS (
SELECT
    block_timestamp,
    block_id,
    method_names,
    tx_hash,
    tx_receiver,
    tx_signer,
    event_data: data[0]: old_owner_id::string AS old_owner,
    event_data: data[0]: new_owner_id::string AS new_owner,
    event_data: data[0]: amount::integer AS amount,
    _ingested_at,
    _inserted_timestamp
FROM extract_data
)

SELECT * FROM final
