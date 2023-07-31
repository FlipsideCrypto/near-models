{{ config(
    materialized = "incremental",
    cluster_by = ["block_timestamp::DATE", "_load_timestamp::DATE"],
    unique_key = "CONCAT_WS('-', action_id, nft_address)",
    incremental_strategy = "delete+insert",
    tags = ['curated'],
    enabled = False
) }}
--Data pulled from action_events_function_call
WITH function_call AS (

    SELECT
        action_id,
        tx_hash,
        block_id,
        block_timestamp,
        TRY_PARSE_JSON(args) AS args_json,
        method_name,
        deposit / pow(
            10,
            24
        ) AS deposit,
        _load_timestamp,
        CASE
            WHEN args_json :receiver_id IS NOT NULL THEN args_json :receiver_id :: STRING
            WHEN args_json :receiver_ids IS NOT NULL THEN args_json :receiver_ids :: STRING
        END AS project_name,
        CASE
            WHEN args_json :token_series_id IS NOT NULL THEN TRY_PARSE_JSON(
                args_json :token_series_id
            ) :: STRING
            WHEN args_json :token_owner_id IS NOT NULL THEN TRY_PARSE_JSON(
                args_json :token_series_id
            ) :: STRING
        END AS nft_id,
        TRY_PARSE_JSON(
            args_json :token_id
        ) :: STRING AS token_id
    FROM
        {{ ref("silver__actions_events_function_call_s3") }}
    WHERE
        method_name IN (
            'nft_mint',
            'nft_mint_batch'
        ) 
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter("_load_timestamp") }}
        {% endif %}
),
--Data Pulled from Transaction
mint_transactions AS (
    SELECT
        tx_hash,
        tx_signer,
        tx_receiver,
        transaction_fee / pow(
            10,
            24
        ) AS network_fee,
        tx_status -- tx:actions[0]:functioncall:method_name::string as method_name
    FROM
        {{ ref("silver__streamline_transactions_final") }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                function_call
        )
        AND tx_status = 'Success' 
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter("_load_timestamp") }}
        {% endif %}
),
--Data pulled from Receipts Table
receipts_data AS (
    SELECT
        tx_hash,
        receipt_index,
        receipt_object_id AS receipt_id,
        receipt_outcome_id :: STRING AS receipt_outcome_id,
        receiver_id,
        gas_burnt
    FROM
        {{ ref("silver__streamline_receipts_final") }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                function_call
        ) 
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter("_load_timestamp") }}
        {% endif %}
)
SELECT
    DISTINCT action_id,
    function_call.tx_hash,
    block_id,
    block_timestamp,
    method_name,
    _load_timestamp,
    tx_signer,
    tx_receiver,
    project_name,
    token_id,
    nft_id,
    receipts_data.receiver_id AS nft_address,
    network_fee,
    tx_status
FROM
    function_call
    LEFT JOIN mint_transactions
    ON function_call.tx_hash = mint_transactions.tx_hash
    LEFT JOIN receipts_data
    ON function_call.tx_hash = receipts_data.tx_hash
WHERE
    tx_status IS NOT NULL
