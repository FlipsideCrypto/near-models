{{ config(
    materialized = "incremental",
    unique_key = "action_id",
    incremental_strategy = "delete+insert",
    cluster_by = ["block_timestamp::DATE", "_inserted_timestamp::DATE"],
) }}

WITH actions AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        args,
        NULLIF(
            j.value :amount_in,
            NULL
        ) :: bigint AS amount_in,
        NULLIF(
            j.value :min_amount_out,
            NULL
        ) :: bigint AS amount_out,
        NULLIF(
            j.value :pool_id,
            NULL
        ) :: text AS pool_id,
        NULLIF(
            j.value :token_in,
            NULL
        ) :: text AS token_in,
        NULLIF(
            j.value :token_out,
            NULL
        ) :: text AS token_out,
        j.index AS swap_index,
        _inserted_timestamp
    FROM
        {{ ref("silver__actions_events_function_call") }},
        LATERAL FLATTEN(input => PARSE_JSON(args) :actions) j
    WHERE
        method_name = 'swap'
        AND args LIKE '%actions%'
        AND {{ incremental_load_filter("_inserted_timestamp") }}
),
receipts AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        CASE
            WHEN PARSE_JSON(
                receipts.status_value
            ) :Failure IS NOT NULL THEN 'Fail'
            ELSE 'Success'
        END AS success_or_fail,
        logs
    FROM
        {{ ref("silver__receipts") }}
    WHERE
        {{ incremental_load_filter("_inserted_timestamp") }}
),
transactions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        tx_signer,
        tx_receiver
    FROM
        {{ ref("silver__transactions") }}
    WHERE
        {{ incremental_load_filter("_inserted_timestamp") }}
),
final_table AS (
    SELECT
        DISTINCT actions.swap_index,
        actions.block_id,
        actions.block_timestamp,
        actions.tx_hash,
        actions.action_id,
        transactions.tx_signer,
        transactions.tx_receiver,
        LAST_VALUE(
            receipts.success_or_fail
        ) over (
            PARTITION BY receipts.tx_hash
            ORDER BY
                receipts.success_or_fail DESC
        ) AS txn_status,
        actions.pool_id,
        actions.amount_in,
        actions.amount_out,
        actions.token_in,
        actions.token_out,
        actions._inserted_timestamp
    FROM
        actions
        JOIN receipts
        ON actions.tx_hash = receipts.tx_hash
        JOIN transactions
        ON actions.tx_hash = transactions.tx_hash
    ORDER BY
        tx_hash,
        swap_index
)
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    action_id,
    tx_signer,
    tx_receiver,
    pool_id,
    token_in,
    amount_in,
    token_out,
    amount_out,
    swap_index,
    _inserted_timestamp
FROM
    final_table
WHERE
    txn_status = 'Success'
ORDER BY
    tx_hash,
    swap_index
