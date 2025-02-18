-- depends_on: {{ ref('bronze__blocks') }}
-- depends_on: {{ ref('bronze__FR_blocks') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_id",
    cluster_by = "ROUND(block_id, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_id)",
    tags = ['streamline_complete']
) }}

SELECT
    VALUE :BLOCK_ID :: INT AS block_id,
    TO_TIMESTAMP_NTZ(DATA :header :timestamp :: INT, 3) AS block_timestamp,
    DATA :header :hash :: STRING AS block_hash,
    DATA :header :prev_height :: INT AS prev_height,
    DATA :header :prev_hash :: STRING AS prev_hash,
    DATA :header :chunks_included :: INT AS chunks_expected,
    ARRAY_SIZE(
        DATA :chunks :: ARRAY
    ) AS chunks_included,
    {{ target.database }}.streamline.udf_extract_hash_array(
        DATA :chunks :: ARRAY,
        'chunk_hash'
    ) AS chunk_ids,
    partition_key,
    _inserted_timestamp,
    DATA :header :hash :: STRING AS complete_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__blocks') }}
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
    {{ ref('bronze__FR_blocks') }}
WHERE
    typeof(DATA) != 'NULL_VALUE'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_id
ORDER BY
    _inserted_timestamp DESC)) = 1
