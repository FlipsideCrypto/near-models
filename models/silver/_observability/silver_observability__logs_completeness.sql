{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = False,
    tags = ['observability', 'deprecated']
) }}
-- TODO this can be deprecated. Not a good metric of completeness.
WITH summary_stats AS (

    SELECT
        MIN(block_id) AS min_block,
        MAX(block_id) AS max_block,
        MIN(block_timestamp) AS min_block_timestamp,
        MAX(block_timestamp) AS max_block_timestamp,
        COUNT(1) AS blocks_tested
    FROM
        {{ ref('silver__blocks_final') }}
    WHERE
        block_timestamp <= DATEADD('hour', -12, SYSDATE())

{% if is_incremental() %}
AND (
    block_id >= (
        SELECT
            MIN(block_id)
        FROM
            (
                SELECT
                    MIN(block_id) AS block_id
                FROM
                    {{ ref('silver__blocks_final') }}
                WHERE
                    block_timestamp BETWEEN DATEADD('hour', -96, SYSDATE())
                    AND DATEADD('hour', -95, SYSDATE())
                UNION
                SELECT
                    MIN(VALUE) - 1 AS block_id
                FROM
                    (
                        SELECT
                            blocks_impacted_array
                        FROM
                            {{ this }}
                            qualify ROW_NUMBER() over (
                                ORDER BY
                                    test_timestamp DESC
                            ) = 1
                    ),
                    LATERAL FLATTEN(
                        input => blocks_impacted_array
                    )
            )
    ) {% if var('OBSERV_FULL_TEST') %}
        OR block_id >= 9820210
    {% endif %}
)
{% endif %}
),
block_range AS (
    SELECT
        _id AS block_id
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence') 
        }}
    WHERE
        _id BETWEEN (
            SELECT
                min_block
            FROM
                summary_stats
        )
        AND (
            SELECT
                max_block
            FROM
                summary_stats
        )
),
broken_blocks AS (
    SELECT
        DISTINCT block_id as block_id
    FROM
        {{ ref('silver__receipts_final') }}
        r
        LEFT JOIN {{ ref('silver__logs_s3') }}
        l USING (
            block_id,
            tx_hash
        )
        JOIN block_range USING (block_id)
    WHERE
        l.tx_hash IS NULL
        AND ARRAY_SIZE(
            r.outcome_json :outcome :logs :: ARRAY
        ) > 0
        AND r.block_id IS NOT NULL
),
impacted_blocks AS (
    SELECT
        COUNT(1) AS blocks_impacted_count,
        ARRAY_AGG(block_id) within GROUP (
            ORDER BY
                2
        ) AS blocks_impacted_array
    FROM
        broken_blocks
)
SELECT
    'event_logs' AS test_name,
    min_block,
    max_block,
    min_block_timestamp,
    max_block_timestamp,
    blocks_tested,
    blocks_impacted_count,
    blocks_impacted_array,
    SYSDATE() AS test_timestamp
FROM
    summary_stats
    JOIN impacted_blocks
    ON 1 = 1
