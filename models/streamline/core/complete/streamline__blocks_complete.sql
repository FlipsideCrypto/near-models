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
    VALUE :block_number :: INT AS block_number,
    DATA :header :hash :: STRING AS block_hash,
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
{% else %}
    {{ ref('bronze__FR_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
