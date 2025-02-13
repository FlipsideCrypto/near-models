-- depends_on: {{ ref('silver__streamline_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = False,
    tags = ['observability']
) }}


WITH block_chunks_included AS (

    SELECT
        block_id,
        block_timestamp,
        header :chunks_included :: INT AS chunks_included,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_blocks') }} -- Streamline Migration TODO - change this to fact blocks once table
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
                    {{ ref('silver__streamline_blocks') }} -- Streamline Migration TODO - change this to fact blocks once table
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
summary_stats AS (

    SELECT
        MIN(block_id) AS min_block,
        MAX(block_id) AS max_block,
        MIN(block_timestamp) AS min_block_timestamp,
        MAX(block_timestamp) AS max_block_timestamp,
        COUNT(1) AS blocks_tested
    FROM
        block_chunks_included
),
chunks_per_block AS (
    SELECT
        block_id,
        MAX(_inserted_timestamp) AS _inserted_timestamp,
        COUNT(
            DISTINCT chunk :header :chunk_hash :: STRING
        ) AS chunk_ct
    FROM
        {{ ref('silver__streamline_shards') }} -- Streamline Migration TODO - change this to fact shards once table
    WHERE 
        block_id >= (SELECT min_block FROM summary_stats) 
    AND
        block_id <= (SELECT max_block FROM summary_stats)
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
),
impacted_blocks AS (
    SELECT
        COUNT(1) AS blocks_impacted_count,
        ARRAY_AGG(BBLOCK_ID) within GROUP (
            ORDER BY
                2
        ) AS blocks_impacted_array
    FROM
        missing
)

SELECT
    'chunk' AS test_name,
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
