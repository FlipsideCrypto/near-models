{{ config(
    severity = "error"
) }}

WITH blocks AS (

    SELECT
        block_id,
        header :chunks_included :: INT AS chunk_ct_expected,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_blocks') }}

        {% if var('DBT_FULL_TEST') %}
        WHERE
            _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
        WHERE
            _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
),
shards AS (
    SELECT
        block_id,
        MAX(_inserted_timestamp) AS _inserted_timestamp,
        COUNT(
            DISTINCT chunk :header :chunk_hash :: STRING
        ) AS chunk_ct_actual
    FROM
        {{ ref('silver__streamline_shards') }}

        {% if var('DBT_FULL_TEST') %}
        WHERE
            _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
        WHERE
            _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
    GROUP BY
        1
),
comp AS (
    SELECT
        b.block_id AS b_block_id,
        s.block_id AS s_block_id,
        b.chunk_ct_expected,
        s.chunk_ct_actual,
        _partition_by_block_number,
        b._inserted_timestamp AS _inserted_timestamp_blocks,
        s._inserted_timestamp AS _inserted_timestamp_shards
    FROM
        blocks b full
        OUTER JOIN shards s USING (block_id)
)
SELECT
    COALESCE(
        b_block_id,
        s_block_id
    ) AS block_id,
    chunk_ct_expected,
    chunk_ct_actual,
    _partition_by_block_number,
    (
        chunk_ct_actual != chunk_ct_expected
        OR b_block_id IS NULL
        OR s_block_id IS NULL
    ) AS is_missing
FROM
    comp
WHERE
    chunk_ct_expected > 0
    AND is_missing 
    {# Filter out false positive from blocks at start of window #}
    AND _inserted_timestamp_blocks > SYSDATE() - INTERVAL '7 days' + INTERVAL '1 hour'
    AND _inserted_timestamp_shards > SYSDATE() - INTERVAL '7 days' + INTERVAL '1 hour'
ORDER BY
    1
