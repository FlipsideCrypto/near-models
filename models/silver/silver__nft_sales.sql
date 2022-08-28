{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
),
tx AS (
    SELECT
        tx_receiver,
        tx_signer,
        transaction_fee,
        tx_status,
        tx: actions [0]: FunctionCall: method_name :: STRING AS method_names,
        tx: actions [0]: FunctionCall: args :: STRING AS args,
        _ingested_at,
        _inserted_timestamp
    FROM
        txs
),
usn_tx AS (
    SELECT
        *,
        COALESCE(TRY_PARSE_JSON(TRY_BASE64_DECODE_STRING(args)), TRY_BASE64_DECODE_STRING(args), args) AS args_decoded
    FROM
        tx
),
nft_transfer AS (
    SELECT
        PARSE_JSON(args_decoded) AS parsed_args,
        tx_signer,
        tx_status,
        tx_receiver,
        transaction_fee,
        _ingested_at,
        _inserted_timestamp
    FROM
        usn_tx
    WHERE
        method_names = 'nft_transfer_call'
    OR method_names = 'nft_transfer'
),
final AS (
    SELECT
        nvl(parsed_args: receiver_id :: STRING,'') AS buyer,
        tx_signer AS seller,
        tx_status,
        tx_receiver AS nft_project,
        nvl(parsed_args: token_id :: VARCHAR, '') AS nft_id,
        nvl(transaction_fee,0) AS network_fee,
        _ingested_at,
        _inserted_timestamp
    FROM
        nft_transfer
)
SELECT
    *
FROM
    final
