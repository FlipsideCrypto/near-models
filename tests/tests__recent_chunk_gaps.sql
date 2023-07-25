{{ config(
    severity = 'warn',
    tags = ['recent_gap_test']
) }}

WITH recent_blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_blocks') }}
    WHERE
        block_timestamp :: DATE >= CURRENT_DATE - INTERVAL '2 days'
),
recent_chunks AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_chunks') }}
    WHERE
        block_id >= (
            SELECT
                MIN(block_id)
            FROM
                recent_blocks
        )
),
block_chunks_included AS (
    SELECT
        block_id,
        header :chunks_included AS chunk_count_block_header,
        _partition_by_block_number
    FROM
        recent_blocks
),
chunks_per_block AS (
    SELECT
        block_id,
        COUNT(
            DISTINCT chunk_hash
        ) AS chunk_count_actual
    FROM
        recent_chunks
    GROUP BY
        1
),
comp AS (
    SELECT
        _partition_by_block_number,
        b.block_id AS bblock_id,
        C.block_id AS cblock_id,
        b.chunk_count_block_header,
        C.chunk_count_actual
    FROM
        block_chunks_included b full
        OUTER JOIN chunks_per_block C USING (block_id)
),
missing AS (
    SELECT
        *
    FROM
        comp
    WHERE
        chunk_count_block_header > 0
        AND (
            bblock_id IS NULL
            OR cblock_id IS NULL
            OR chunk_count_block_header != chunk_count_actual
        )
    ORDER BY
        1
),
FINAL AS (
    SELECT
        bblock_id AS block_id,
        _partition_by_block_number,
        chunk_count_block_header,
        chunk_count_actual,
        CURRENT_TIMESTAMP AS _test_timestamp
    FROM
        missing
    ORDER BY
        1
)
SELECT
    *
FROM
    FINAL
