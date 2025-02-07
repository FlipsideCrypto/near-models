{{ config (
    materialized = "incremental",
    unique_key = "chunk_hash",
    cluster_by = "ROUND(block_id, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(chunk_hash, block_id)",
    tags = ['streamline_helper']
) }}

WITH blocks_complete AS (

    SELECT
        block_id,
        block_hash,
        chunk_ids,
        _inserted_timestamp
    FROM
        {{ ref('streamline__blocks_complete') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
flatten_chunk_ids AS (
    SELECT
        block_id,
        block_hash,
        VALUE :: STRING AS chunk_hash,
        INDEX AS chunk_index,
        _inserted_timestamp
    FROM
        blocks_complete,
        LATERAL FLATTEN(
            input => chunk_ids
        )
)
SELECT
    block_id,
    block_hash,
    chunk_hash,
    chunk_index,
    _inserted_timestamp,
    chunk_hash AS chunks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_chunk_ids
    
qualify(ROW_NUMBER() over (PARTITION BY chunk_hash
ORDER BY
    modified_timestamp DESC)) = 1
