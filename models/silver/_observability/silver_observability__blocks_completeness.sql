-- depends_on: {{ ref('silver__streamline_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = False,
    tags = ['observability']
) }}

WITH blocks_joined AS (

    SELECT
        A.block_id AS current_block_id,
        A.block_timestamp AS current_block_timestamp,
        A.block_hash AS current_block_hash,
        b.block_id AS next_block_id,
        b.block_timestamp AS next_block_timestamp,
        b.prev_hash AS next_prev_hash
    FROM
        {{ ref('silver__streamline_blocks') }} A
        LEFT JOIN {{ ref('silver__streamline_blocks') }}
        b
        ON A.block_hash = b.prev_hash
    WHERE
        A.block_timestamp < b.block_timestamp -- Ensuring temporal order
        AND A.block_timestamp <= DATEADD('hour', -12, SYSDATE())

{% if is_incremental() %}
AND (
    A.block_id >= (
        SELECT
            MIN(block_id)
        FROM
            (
                SELECT
                    MIN(block_id) AS block_id
                FROM
                    {{ ref('silver__streamline_blocks') }}
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
        OR b.block_id >= 9820210
    {% endif %}
)
{% endif %}
),
blocks_impacted AS (
    SELECT
        current_block_id,
        current_block_timestamp,
        current_block_hash
    FROM
        blocks_joined
    WHERE
        next_block_id IS NULL -- Where there is no next block
        OR current_block_hash != next_prev_hash -- Or the hash doesn't match
),
aggregated_data AS (
    SELECT
        MIN(
            A.current_block_id
        ) AS min_block,
        MAX(
            A.current_block_id
        ) AS max_block,
        MIN(
            A.current_block_timestamp
        ) AS min_block_timestamp,
        MAX(
            A.current_block_timestamp
        ) AS max_block_timestamp,
        COUNT(
            DISTINCT A.current_block_id
        ) AS blocks_tested,
        COUNT(
            DISTINCT b.current_block_id
        ) AS blocks_impacted_count,
        ARRAY_AGG(
            DISTINCT b.current_block_id
        ) AS blocks_impacted_array
    FROM
        blocks_joined A
        LEFT JOIN blocks_impacted b
        ON A.current_block_id = b.current_block_id
)
SELECT
    'blocks' AS test_name,
    min_block,
    max_block,
    min_block_timestamp,
    max_block_timestamp,
    blocks_tested,
    blocks_impacted_count,
    blocks_impacted_array,
    SYSDATE() AS test_timestamp
FROM
    aggregated_data
