{{ config(
    severity = "error"
) }}

WITH deposit_functioncalls AS (

    SELECT
        DISTINCT action_id,
        block_id,
        block_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        deposit :: INT > 0
        AND receipt_succeeded

        {% if var('DBT_FULL_TEST') %}
        AND _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
    {% else %}
        AND _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
        AND SYSDATE() - INTERVAL '1 hour'
    {% endif %}
),
native_transfer_deposits AS (
    SELECT
        DISTINCT action_id,
        block_id,
        block_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_deposit') }}

        {% if var('DBT_FULL_TEST') %}
        WHERE
            _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
        WHERE
            _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
)
SELECT
    A.action_id,
    A.block_id,
    A.block_timestamp,
    A._partition_by_block_number
FROM
    deposit_functioncalls A
    LEFT JOIN native_transfer_deposits b
    ON A.action_id = b.action_id
WHERE
    b.action_id IS NULL
