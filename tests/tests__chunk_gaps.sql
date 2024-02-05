{{ config(
    error_if = '>=25',
    warn_if = 'BETWEEN 1 AND 24'
) }}

WITH block_chunks_included AS (

    SELECT
        block_id,
        header :chunks_included AS chunks_included,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_blocks') }}
),
chunks_per_block AS (
    SELECT
        block_id,
        MAX(_inserted_timestamp) AS _inserted_timestamp,
        COUNT(
            DISTINCT chunk_hash
        ) AS chunk_ct
    FROM
        {{ ref('silver__streamline_chunks') }}
    GROUP BY
        1
),
comp AS (
    SELECT
        _partition_by_block_number,
        b.block_id AS bblock_id,
        C.block_id AS cblock_id,
        b.chunks_included,
        C.chunk_ct,
        b._inserted_timestamp AS b_inserted_timestamp,
        C._inserted_timestamp AS c_inserted_timestamp
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
        AND b_inserted_timestamp <= SYSDATE() - INTERVAL '1 hour'
        AND c_inserted_timestamp <= SYSDATE() - INTERVAL '1 hour'
    ORDER BY
        1
)
SELECT
    *
FROM
    missing
