-- depends_on: {{ ref('bronze__transactions') }}
-- depends_on: {{ ref('bronze__FR_transactions') }}
{{ config (
    materialized = "incremental",
    unique_key = "tx_hash",
    cluster_by = "ROUND(block_id, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash)",
    tags = ['streamline_complete']
) }}

SELECT
    VALUE :BLOCK_ID :: INT AS block_id,
    VALUE :BLOCK_TIMESTAMP :: timestamp_ntz AS block_timestamp,
    VALUE :TX_HASH :: STRING AS tx_hash,
    DATA :transaction :signer_id :: STRING AS signer_id,
    partition_key,
    _inserted_timestamp,
    DATA :transaction :hash :: STRING AS complete_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__transactions') }}
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
    {{ ref('bronze__FR_transactions') }}
WHERE
    typeof(DATA) != 'NULL_VALUE'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_hash
ORDER BY
    _inserted_timestamp DESC)) = 1
