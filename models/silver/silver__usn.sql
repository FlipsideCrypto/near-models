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
        TRY_PARSE_JSON(
            TRY_BASE64_DECODE_STRING(
                tx: actions[0]: FunctionCall: args
            )
        ) AS parsed_arg,
        tx: actions[0]: FunctionCall: method_name :: STRING AS method_names,
        tx,
        _ingested_at,
        _inserted_timestamp
    FROM
        transactions
    WHERE
        tx_receiver = 'usn'
        OR tx_signer = 'usn'
),
buy AS (
    SELECT
        block_timestamp,
        block_id,
        method_names,
        tx_hash,
        tx_receiver,
        tx_signer,
        tx: actions[0]: FunctionCall: deposit :: NUMBER AS amount,
        _ingested_at,
        _inserted_timestamp
    FROM
        usn_tx
    WHERE
        method_names = 'buy'
),
sell AS (
    SELECT
        block_timestamp,
        block_id,
        method_names,
        tx_hash,
        tx_receiver,
        tx_signer,
        parsed_arg: amount :: NUMBER AS amount,
        _ingested_at,
        _inserted_timestamp
    FROM
        usn_tx
    WHERE
        method_names = 'sell'
),
FINAL AS (
    SELECT
        *
    FROM
        buy
    UNION
    SELECT
        *
    FROM
        sell
)
SELECT
    *
FROM
    FINAL
