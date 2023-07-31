{{ config(
    severity = 'error'
) }}

WITH block_chunks_included AS (

    SELECT
        block_id,
        header :chunks_included AS chunks_included,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_blocks') }}
    WHERE
        _inserted_timestamp <= (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ ref('silver__streamline_blocks') }}
        ) - INTERVAL '1 hour'
),
chunks_per_block AS (
    SELECT
        block_id,
        COUNT(
            DISTINCT chunk_hash
        ) AS chunk_ct
    FROM
        {{ ref('silver__streamline_chunks') }}
    WHERE
        _inserted_timestamp <= (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ ref('silver__streamline_chunks') }}
        ) - INTERVAL '1 hour'
    GROUP BY
        1
),
comp AS (
    SELECT
        _partition_by_block_number,
        b.block_id AS bblock_id,
        C.block_id AS cblock_id,
        b.chunks_included,
        C.chunk_ct
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
        chunks_included > 0
        AND (
            bblock_id IS NULL
            OR cblock_id IS NULL
            OR chunks_included != chunk_ct
        )
    ORDER BY
        1
),
FINAL AS (
    SELECT
        _partition_by_block_number,
        ARRAY_AGG(bblock_id) AS blocks_to_walk,
        ARRAY_SIZE(blocks_to_walk) AS impacted_block_ct
    FROM
        missing
    GROUP BY
        1
    ORDER BY
        1
)
SELECT
    _partition_by_block_number,
    blocks_to_walk,
    impacted_block_ct,
    CURRENT_TIMESTAMP AS _test_timestamp
FROM
    FINAL
