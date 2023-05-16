{{ config(
    enabled = False
) }}

WITH actual AS (

    SELECT
        tx_hash
    FROM
        {{ ref('silver__dex_swaps') }}
),
success_txs AS (
    SELECT
        tx_hash
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_status = 'Success'
),
expected AS (
    SELECT
        tx_hash,
        IFF(
            method_name = 'ft_transfer_call',
            TRY_PARSE_JSON(TRY_PARSE_JSON(args) :msg),
            TRY_PARSE_JSON(args)
        ) :actions AS actions
    FROM
        {{ ref('silver__actions_events_function_call') }}
    WHERE
        method_name IN (
            'ft_transfer_call',
            'swap'
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                success_txs
        )
        AND actions IS NOT NULL
        AND ARRAY_SIZE(actions) > 0
),
in_both AS (
    SELECT
        tx_hash
    FROM
        expected
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                actual
        )
),
BOTH AS (
    SELECT
        tx_hash
    FROM
        expected
    UNION
    SELECT
        tx_hash
    FROM
        actual
)
SELECT
    tx_hash
FROM
    BOTH
WHERE
    tx_hash NOT IN (
        SELECT
            tx_hash
        FROM
            in_both
    )
