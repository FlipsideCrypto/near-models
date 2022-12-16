with swaps as (
    select
        swap_id,
        tx_hash,
        token_in,
        amount_in,
        token_out,
        amount_out
    from {{ ref('silver__dex_swaps') }}
),

swap_logs as (
    select
        tx_hash,
        tx:actions[0]:FunctionCall:method_name as method_name,
        iff(method_name = 'ft_transfer_call',
            tx:receipt[1]:outcome:logs,
            tx:receipt[0]:outcome:logs
        ) as logs
    from {{ ref('silver__transactions') }}
    where tx_hash in (select tx_hash from swaps)
),

expected as (
    select
        concat(tx_hash, '-', row_number() over (partition by tx_hash order by index asc) - 1) as swap_id,
        regexp_substr(value, 'Swapped (\\d+)', 1, 1, 'e')::number as amount_in,
        regexp_substr(value, 'Swapped \\d+ (.+) for ', 1, 1, 'e') as token_in,
        regexp_substr(value, 'Swapped \\d+ .+ for (\\d+)', 1, 1, 'e')::number as amount_out,
        regexp_substr(value, 'Swapped \\d+ .+ for \\d+ ([^,]+)', 1, 1, 'e') as token_out
    from swap_logs,
    lateral flatten(input => logs)
    where amount_in is not null
       or token_in is not null
       or amount_out is not null
       or amount_in is not null
),

expected_adjusted as (
    select
        swap_id,
        token_in,
        amount_in / pow(10, labels_in.decimals) as amount_in,
        token_out,
        amount_out / pow(10, labels_out.decimals) as amount_out 
    from expected
    inner join token_labels labels_in on expected.token_in = labels_in.token_contract
    inner join token_labels labels_out on expected.token_out = labels_out.token_contract
),

test as (
    select
        expected_adjusted.swap_id,
        expected_adjusted.amount_in as expected_amount_in,
        expected_adjusted.token_in as expected_token_in,
        swaps.amount_in as actual_amount_in,
        swaps.token_in as actual_token_in,
        expected_adjusted.amount_out as expected_amount_out,
        expected_adjusted.token_out as expected_token_out,
        swaps.amount_out as actual_amount_out,
        swaps.token_out as actual_token_out
    from expected_adjusted
    inner join swaps on expected_adjusted.swap_id = swaps.swap_id
    where expected_adjusted.amount_in != swaps.amount_in
       or expected_adjusted.token_in != swaps.token_in
       or expected_adjusted.amount_out != swaps.amount_out
       or expected_adjusted.token_out != swaps.token_out
)

select * from test
