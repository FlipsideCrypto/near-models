{{ config(
    materialized = "incremental",
    unique_key = "swap_id",
    incremental_strategy = "delete+insert",
    cluster_by = ["block_timestamp::DATE", "_load_timestamp::DATE"],
    tags = ['curated', 'curated_s3']
) }}

WITH base_swap_calls AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        args,
        _load_timestamp,
        method_name
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        method_name IN (
            'swap',
            'ft_transfer_call'
        )
        AND {{ incremental_load_filter('_load_timestamp') }}
),
base_swaps AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        IFF(
            method_name = 'ft_transfer_call',
            TRY_PARSE_JSON(TRY_PARSE_JSON(args) :msg),
            TRY_PARSE_JSON(args)
        ) :actions AS actions,
        _load_timestamp
    FROM
        base_swap_calls
),
agg_swaps AS (
    SELECT
        tx_hash,
        ANY_VALUE(block_id) AS block_id,
        ANY_VALUE(block_timestamp) AS block_timestamp,
        ARRAY_AGG(
            action.value
        ) within GROUP (
            ORDER BY
                action_id,
                action.index
        ) AS action_list,
        ANY_VALUE(_load_timestamp) AS _load_timestamp
    FROM
        base_swaps,
        LATERAL FLATTEN(
            input => actions
        ) action
    GROUP BY
        1
),
actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        NULLIF(
            action.value :pool_id,
            NULL
        ) AS pool_id,
        NULLIF(
            action.value :token_in,
            NULL
        ) :: text AS token_in,
        NULLIF(
            action.value :token_out,
            NULL
        ) :: text AS token_out,
        action.index AS swap_index,
        _load_timestamp
    FROM
        agg_swaps,
        LATERAL FLATTEN(
            input => action_list
        ) action
    WHERE
        NOT RLIKE(
            pool_id,
            '.*[a-z].*',
            'i'
        )
),
receipts AS (
    SELECT
        block_id,
        tx_hash,
        CASE
            WHEN PARSE_JSON(
                r.status_value
            ) :Failure IS NOT NULL THEN 'Fail'
            ELSE 'Success'
        END AS success_or_fail,
        logs
    FROM
        {{ ref("silver__streamline_receipts_final") }}
        r
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                actions
        )
),
flat_receipts AS (
    SELECT
        tx_hash,
        l.value,
        l.index,
        success_or_fail
    FROM
        receipts,
        LATERAL FLATTEN(
            input => logs
        ) l
),
swap_logs AS (
    SELECT
        tx_hash,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                INDEX ASC
        ) - 1 AS swap_index,
        VALUE,
        success_or_fail
    FROM
        flat_receipts
    WHERE
        VALUE LIKE 'Swapped%'
),
transactions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        tx_signer,
        tx_receiver
    FROM
        {{ ref("silver__streamline_transactions_final") }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                actions
        )
),
token_labels AS (
    SELECT
        *
    FROM
        {{ ref("silver__token_labels") }}
),
final_table AS (
    SELECT
        swap_logs.swap_index,
        actions._load_timestamp,
        actions.block_id,
        actions.block_timestamp,
        swap_logs.tx_hash,
        CONCAT(
            swap_logs.tx_hash,
            '-',
            swap_logs.swap_index
        ) AS swap_id,
        swap_logs.value AS log_data,
        transactions.tx_signer AS trader,
        transactions.tx_receiver AS platform,
        LAST_VALUE(
            swap_logs.success_or_fail
        ) over (
            PARTITION BY swap_logs.tx_hash
            ORDER BY
                swap_logs.success_or_fail DESC
        ) AS txn_status,
        actions.pool_id :: INT AS pool_id,
        actions.token_in,
        actions.token_out
    FROM
        actions
        INNER JOIN swap_logs
        ON (
            swap_logs.tx_hash = actions.tx_hash
            AND swap_logs.swap_index = actions.swap_index
        )
        JOIN transactions
        ON actions.tx_hash = transactions.tx_hash
),
FINAL AS (
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
        REGEXP_SUBSTR(
            log_data,
            'Swapped (\\d+)',
            1,
            1,
            'e'
        ) :: NUMBER / pow(10, IFNULL(token_labels_in.decimals, 0)) AS amount_in,
        token_out,
        token_labels_out.symbol AS token_out_symbol,
        REGEXP_SUBSTR(
            log_data,
            'Swapped \\d+ .+ for (\\d+)',
            1,
            1,
            'e'
        ) :: NUMBER / pow(10, IFNULL(token_labels_out.decimals, 0)) AS amount_out,
        swap_index,
        _load_timestamp
    FROM
        final_table
        LEFT JOIN token_labels AS token_labels_in
        ON final_table.token_in = token_labels_in.token_contract
        LEFT JOIN token_labels AS token_labels_out
        ON final_table.token_out = token_labels_out.token_contract
    WHERE
        txn_status = 'Success'
        AND log_data IS NOT NULL
)
SELECT
    *
FROM
    FINAL
