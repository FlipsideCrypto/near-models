-- depends_on: {{ ref('bronze__omni_metadata') }}
{{ config(
    materialized = "incremental",
    unique_key = "omni_complete_id",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_non_core']
) }}

SELECT
    VALUE:OMNI_ASSET_IDENTIFIER :: STRING AS omni_asset_identifier,
    partition_key,
    _inserted_timestamp,
    VALUE:OMNI_ASSET_IDENTIFIER :: STRING AS omni_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__omni_metadata') }}
WHERE
    typeof(DATA) != 'NULL_VALUE'

{% if is_incremental() %}
AND
    _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp) _inserted_timestamp
            FROM
                {{ this }}
        ),
        '1900-01-01' :: timestamp_ntz
    )
{% endif %}

qualify(row_number() over (partition by omni_asset_identifier order by _inserted_timestamp desc)) = 1
