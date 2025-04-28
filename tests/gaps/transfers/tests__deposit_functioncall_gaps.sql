{{ config(
    severity = "error",
    tags = ['gap_test'],
    enabled = False
) }}

WITH deposit_functioncalls AS (

    SELECT
        DISTINCT receipt_id,
        action_index,
        block_id,
        block_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        action_data :deposit :: INT > 0
        AND receipt_succeeded

        AND inserted_timestamp BETWEEN SYSDATE() - INTERVAL '{{ var('DBT_TEST_LOOKBACK_DAYS', 14) }} days'
            AND SYSDATE() - INTERVAL '1 hour'

),
native_transfer_deposits AS (
    SELECT
        DISTINCT receipt_id,
        action_index,
        block_id,
        block_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_deposit') }}

        WHERE inserted_timestamp BETWEEN SYSDATE() - INTERVAL '{{ var('DBT_TEST_LOOKBACK_DAYS', 14) }} days'
            AND SYSDATE() - INTERVAL '1 hour'
)
SELECT
    A.receipt_id,
    A.action_index,
    A.block_id,
    A.block_timestamp,
    A._partition_by_block_number
FROM
    deposit_functioncalls A
    LEFT JOIN native_transfer_deposits b
    ON A.receipt_id = b.receipt_id
    AND A.action_index = b.action_index
WHERE
    b.receipt_id IS NULL
