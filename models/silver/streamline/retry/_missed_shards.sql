{{ config(
    materialized = 'ephemeral'
) }}

WITH block_chunks_included AS (

    SELECT
        block_id,
        header :chunks_included :: NUMBER AS chunks_included,
        _partition_by_block_number
    FROM
        {{ ref('silver__streamline_blocks') }}
),
chunks_per_block AS (
    SELECT
        block_id,
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
        C.chunk_ct
    FROM
        block_chunks_included b full
        OUTER JOIN chunks_per_block C USING (block_id)
),
FINAL AS (
    SELECT
        _partition_by_block_number,
        bblock_id AS block_id Í
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
)
SELECT
    *
FROM
    FINAL
