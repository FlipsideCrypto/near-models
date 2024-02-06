{{ config(
    materialized = 'view',
    tags = ['helper', 'receipt_map']
) }}

WITH receipts AS (

    SELECT
        A.receipt_id PARENT,
        b.value :: STRING item,
        block_id,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_receipts') }} A
        JOIN LATERAL FLATTEN(
            A.outcome_receipts,
            outer => TRUE
        ) b

        {% if var("MANUAL_FIX") %}
    WHERE
        {{ partition_load_manual('front') }}
    {% else %}
    WHERE
        _partition_by_block_number >= (
            SELECT
                MIN(_partition_by_block_number) - (3000 * {{ var('RECEIPT_MAP_LOOKBACK_HOURS') }})
            FROM
                {{ ref('_retry_range')}}
        )
    {% endif %}
)
SELECT
    *
FROM
    receipts
