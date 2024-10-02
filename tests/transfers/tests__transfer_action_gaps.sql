{{ config(
    severity = "error"
) }}

WITH transfer_actions AS (

    SELECT
        DISTINCT tx_hash,
        block_id,
        block_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__actions_events_s3') }}
    WHERE
        LOWER(action_name) = 'transfer' {% if var('DBT_FULL_TEST') %}
            AND _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
            AND _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
),
native_transfers AS (
    SELECT
        DISTINCT tx_hash,
        block_id,
        block_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__token_transfer_native')}}
    {% if var('DBT_FULL_TEST')%}
    WHERE 
        _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
    {% else %}
    WHERE
        _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
        AND SYSDATE() - INTERVAL '1 hour'
    {% endif %}
)
select
    a.tx_hash,
    a.block_id,
    a.block_timestamp,
    a._partition_by_block_number
from
    transfer_actions a
    left join native_transfers b
    on a.tx_hash = b.tx_hash
where
    b.tx_hash is null
