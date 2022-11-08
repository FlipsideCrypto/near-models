with actual as (
    select tx_hash from {{ ref('silver__dex_swaps') }}
),

success_txs as (
    select tx_hash
    from {{ ref('silver__transactions') }}
    where tx_status = 'Success'
),

expected as (
    select
        tx_hash,
        iff(method_name = 'ft_transfer_call',
            try_parse_json(try_parse_json(args):msg),
            try_parse_json(args)
        ):actions as actions
    from {{ ref('silver__actions_events_function_call') }}
    where method_name in ('ft_transfer_call', 'swap')
      and tx_hash in (select tx_hash from success_txs)
      and actions is not null
      and array_size(actions) > 0
),

in_both as (
    select
        tx_hash
    from expected
    where tx_hash in (select tx_hash from actual)
),

both as (
    select
        tx_hash
    from expected
    union
    select
        tx_hash
    from actual
)

select
    tx_hash
from both
where tx_hash not in (select tx_hash from in_both)
