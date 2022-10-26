{{ config(
    materialized = "incremental",
    unique_key = "swap_id",
    incremental_strategy = "delete+insert",
    cluster_by = ["block_timestamp::DATE", "_inserted_timestamp::DATE"],
) }}

WITH base_swaps as (
    select
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        parse_json(args):actions as actions,
        _inserted_timestamp
    from {{ ref('silver__actions_events_function_call') }}
    where method_name = 'swap'
      and args like '%actions%'
      and {{ incremental_load_filter('_inserted_timestamp') }}
),

agg_swaps as (
    select
        tx_hash,
        any_value(block_id) as block_id,
        any_value(block_timestamp) as block_timestamp,
        array_agg(action.value) within group (order by action_id, action.index) as action_list,
        any_value(_inserted_timestamp) as _inserted_timestamp
    from
        base_swaps,
        lateral flatten(input => actions) action
    group by 1
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
        _inserted_timestamp
    FROM
        agg_swaps,
        LATERAL FLATTEN(input => action_list) action
    where not rlike(
        pool_id,
        '.*[a-z].*',
        'i'
    )
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
        tx_hash in (select tx_hash from actions)
),

flat_receipts as (
    select
        tx_hash,
        l.value,
        l.index,
        success_or_fail
    from receipts,
    lateral flatten(input => logs) l
),

swap_logs as (
    select
        tx_hash,
        row_number() over (partition by tx_hash order by index asc) - 1 as swap_index,
        value,
        success_or_fail
    from flat_receipts
    where value like 'Swapped%'
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
    where tx_hash in (select tx_hash from actions)
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
        actions._inserted_timestamp,
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
        inner join swap_logs
                on (swap_logs.tx_hash = actions.tx_hash
                and swap_logs.swap_index = actions.swap_index)
        JOIN transactions
        ON actions.tx_hash = transactions.tx_hash
),
final as (
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
        regexp_substr(log_data, 'Swapped (\\d+)', 1, 1, 'e')::number / pow(10, ifnull(token_labels_in.decimals, 0)) as amount_in,
        token_out,
        token_labels_out.symbol AS token_out_symbol,
        regexp_substr(log_data, 'Swapped \\d+ .+ for (\\d+)', 1, 1, 'e')::number / pow(10, ifnull(token_labels_out.decimals, 0)) as amount_out,
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
)

select * from final
