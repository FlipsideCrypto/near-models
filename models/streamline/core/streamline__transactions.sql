-- depends_on: {{ ref('streamline__chunks_complete') }}
{{ config (
    materialized = "incremental",
    unique_key = "tx_hash",
    cluster_by = "ROUND(block_id, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash, block_id)",
    tags = ['streamline_helper']
) }}

WITH chunks_complete AS (

    SELECT
        block_id,
        chunk_hash,
        shard_id,
        transaction_ids,
        signer_ids,
        _inserted_timestamp
    FROM
        {{ ref('streamline__chunks_complete') }}

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
flatten_tx_ids AS (
    SELECT
        block_id,
        chunk_hash,
        shard_id,
        VALUE :: STRING AS tx_hash,
        INDEX AS tx_index,
        signer_ids [INDEX] :: STRING AS signer_id, --TODO what if there is an error and the txs and signer arrays do not match on index?
        _inserted_timestamp
    FROM
        chunks_complete,
        LATERAL FLATTEN(
            input => transaction_ids
        )
)
SELECT
    block_id,
    chunk_hash,
    shard_id,
    tx_hash,
    tx_index,
    signer_id,
    _inserted_timestamp,
    tx_hash AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_tx_ids
    
qualify(ROW_NUMBER() over (PARTITION BY tx_hash
ORDER BY
    modified_timestamp DESC)) = 1
