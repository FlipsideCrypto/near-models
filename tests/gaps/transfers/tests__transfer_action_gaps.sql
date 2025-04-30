{{ config(
    severity = "error",
    tags = ['gap_test'],
    enabled = False
) }}

WITH transfer_actions AS (

    SELECT
        DISTINCT tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        LOWER(action_name) = 'transfer' 
        AND receipt_succeeded
        AND inserted_timestamp BETWEEN SYSDATE() - INTERVAL '{{ var('DBT_TEST_LOOKBACK_DAYS', 14) }} days'
            AND SYSDATE() - INTERVAL '1 hour'
),
native_transfers AS (
    SELECT
        DISTINCT tx_hash,
        receipt_id,
        block_id,
        block_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_native')}}
    WHERE 
        inserted_timestamp BETWEEN SYSDATE() - INTERVAL '{{ var('DBT_TEST_LOOKBACK_DAYS', 14) }} days'
            AND SYSDATE() - INTERVAL '1 hour'
)
select
    a.tx_hash,
    a.receipt_id,
    a.block_id,
    a.block_timestamp,
    a._partition_by_block_number
from
    transfer_actions a
    left join native_transfers b
    on a.tx_hash = b.tx_hash
    and a.receipt_id = b.receipt_id
where
    b.tx_hash is null
