-- depends_on: {{ ref('bronze__blocks') }}
-- depends_on: {{ ref('bronze__FR_blocks') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['streamline_complete']
) }}

SELECT
    VALUE :BLOCK_NUMBER :: INT AS block_number,
    DATA :header :hash :: STRING AS block_hash,
    DATA :header :chunks_included :: INT AS chunks_expected,
    ARRAY_SIZE(
        DATA :chunks :: ARRAY
    ) AS chunks_included,
    {{ target.database }}.streamline.udf_extract_hash_array(
        DATA :chunks :: ARRAY,
        'chunk_hash'
    ) AS chunk_ids,
    -- array_size(chunk_ids) = chunks_included as array_is_complete ?
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
    AND DATA IS NOT NULL
{% else %}
    {{ ref('bronze__FR_blocks') }}
WHERE
    DATA IS NOT NULL
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
