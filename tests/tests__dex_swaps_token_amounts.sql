{{ config(
    enabled = False
) }}

WITH swaps AS (

    SELECT
        swap_id,
        tx_hash,
        token_in,
        amount_in,
        token_out,
        amount_out
    FROM
        {{ ref('silver__dex_swaps') }}
),
swap_logs AS (
    SELECT
        tx_hash,
        tx :actions [0] :FunctionCall :method_name AS method_name,
        IFF(
            method_name = 'ft_transfer_call',
            tx :receipt [1] :outcome :logs,
            tx :receipt [0] :outcome :logs
        ) AS logs
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                swaps
        )
),
expected AS (
    SELECT
        CONCAT(tx_hash, '-', ROW_NUMBER() over (PARTITION BY tx_hash
    ORDER BY
        INDEX ASC) - 1) AS swap_id,
        REGEXP_SUBSTR(
            VALUE,
            'Swapped (\\d+)',
            1,
            1,
            'e'
        ) :: NUMBER AS amount_in,
        REGEXP_SUBSTR(
            VALUE,
            'Swapped \\d+ (.+) for ',
            1,
            1,
            'e'
        ) AS token_in,
        REGEXP_SUBSTR(
            VALUE,
            'Swapped \\d+ .+ for (\\d+)',
            1,
            1,
            'e'
        ) :: NUMBER AS amount_out,
        REGEXP_SUBSTR(
            VALUE,
            'Swapped \\d+ .+ for \\d+ ([^,]+)',
            1,
            1,
            'e'
        ) AS token_out
    FROM
        swap_logs,
        LATERAL FLATTEN(
            input => logs
        )
    WHERE
        amount_in IS NOT NULL
        OR token_in IS NOT NULL
        OR amount_out IS NOT NULL
        OR amount_in IS NOT NULL
),
expected_adjusted AS (
    SELECT
        swap_id,
        token_in,
        amount_in / pow(
            10,
            labels_in.decimals
        ) AS amount_in,
        token_out,
        amount_out / pow(
            10,
            labels_out.decimals
        ) AS amount_out
    FROM
        expected
        INNER JOIN token_labels labels_in
        ON expected.token_in = labels_in.token_contract
        INNER JOIN token_labels labels_out
        ON expected.token_out = labels_out.token_contract
),
test AS (
    SELECT
        expected_adjusted.swap_id,
        expected_adjusted.amount_in AS expected_amount_in,
        expected_adjusted.token_in AS expected_token_in,
        swaps.amount_in AS actual_amount_in,
        swaps.token_in AS actual_token_in,
        expected_adjusted.amount_out AS expected_amount_out,
        expected_adjusted.token_out AS expected_token_out,
        swaps.amount_out AS actual_amount_out,
        swaps.token_out AS actual_token_out
    FROM
        expected_adjusted
        INNER JOIN swaps
        ON expected_adjusted.swap_id = swaps.swap_id
    WHERE
        expected_adjusted.amount_in != swaps.amount_in
        OR expected_adjusted.token_in != swaps.token_in
        OR expected_adjusted.amount_out != swaps.amount_out
        OR expected_adjusted.token_out != swaps.token_out
)
SELECT
    *
FROM
    test
