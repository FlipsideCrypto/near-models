{{ config(
    materialized = "incremental",
    unique_key = "swap_id",
    incremental_strategy = "delete+insert",
    cluster_by = ["block_timestamp::DATE", "_inserted_timestamp::DATE"],
) }}

WITH actions AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        args,
        method_name,
        NULLIF(
            j.value :pool_id,
            NULL
        ) AS pool_id,
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
        AND NOT RLIKE(
            pool_id,
            '.*[a-z].*',
            'i'
        )
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
token_in_labels AS (
    SELECT
        *
    FROM
        {{ ref("silver__token_labels") }}
),
token_out_labels AS (
    SELECT
        *
    FROM
        token_in_labels
),
final_table AS (
    SELECT
        DISTINCT actions.swap_index,
        actions._inserted_timestamp,
        actions.block_id,
        actions.block_timestamp,
        actions.tx_hash,
        CONCAT(
            actions.tx_hash,
            '-',
            actions.swap_index
        ) AS swap_id,
        logs,
        logs [swap_index] AS log_data,
        transactions.tx_signer AS trader,
        transactions.tx_receiver AS platform,
        LAST_VALUE(
            receipts.success_or_fail
        ) over (
            PARTITION BY receipts.tx_hash
            ORDER BY
                receipts.success_or_fail DESC
        ) AS txn_status,
        actions.pool_id :: INT AS pool_id,
        actions.token_in,
        actions.token_out
    FROM
        actions
        JOIN receipts
        ON actions.tx_hash = receipts.tx_hash
        JOIN transactions
        ON actions.tx_hash = transactions.tx_hash
)
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    swap_id,
    platform,
    trader,
    pool_id,
    token_in,
    token_labels_in.symbol AS token_in_symbol,
    regexp_substr(log_data, 'Swapped (\\d+)', 1, 1, 'e')::number / pow(10, token_labels_in.decimals) as amount_in,
    token_out,
    token_labels_out.symbol AS token_out_symbol,
    regexp_substr(log_data, 'Swapped \\d+ .+ for (\\d+)', 1, 1, 'e')::number / pow(10, token_labels_out.decimals) as amount_out,
    swap_index,
    _inserted_timestamp
FROM
    final_table
    LEFT JOIN token_labels AS token_labels_in
    ON final_table.token_in = token_labels_in.token_contract
    LEFT JOIN token_labels AS token_labels_out
    ON final_table.token_out = token_labels_out.token_contract
WHERE
    txn_status = 'Success'
    AND log_data IS NOT NULL
