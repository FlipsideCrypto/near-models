-- depends_on: {{ ref('bronze__chunks') }}
-- depends_on: {{ ref('bronze__FR_chunks') }}
{{ config (
    materialized = "incremental",
    unique_key = "chunk_hash",
    cluster_by = "ROUND(block_id, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(chunk_hash)",
    tags = ['streamline_complete']
) }}

SELECT
    VALUE :BLOCK_ID :: INT AS block_id,
    VALUE :BLOCK_TIMESTAMP_EPOCH :: INT AS block_timestamp_epoch,
    VALUE :CHUNK_HASH :: STRING AS chunk_hash,
    DATA :header: shard_id :: INT AS shard_id,
    ARRAY_SIZE(
        DATA :receipts :: ARRAY
    ) AS receipts_count,
    ARRAY_SIZE(
        DATA :transactions :: ARRAY
    ) AS transactions_count,
    {{ target.database }}.streamline.udf_extract_hash_array(
        DATA :transactions :: ARRAY,
        'hash'
    ) AS transaction_ids,
    {{ target.database }}.streamline.udf_extract_hash_array(
        DATA :transactions :: ARRAY,
        'signer_id'
    ) AS signer_ids,
    partition_key,
    _inserted_timestamp,
    DATA :header :chunk_hash :: STRING AS complete_chunks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__chunks') }}
WHERE
    _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp) _inserted_timestamp
            FROM
                {{ this }}
        ),
        '1900-01-01' :: timestamp_ntz
    )
    AND typeof(DATA) != 'NULL_VALUE'
{% else %}
    {{ ref('bronze__FR_chunks') }}
WHERE
    typeof(DATA) != 'NULL_VALUE'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY chunk_hash
ORDER BY
    _inserted_timestamp DESC)) = 1
