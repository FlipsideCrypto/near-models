-- depends_on: {{ ref('bronze__nearblocks_ft_metadata') }}

{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['streamline_non_core']
) }}

{% if var('NEAR_MIGRATE_ARCHIVE', false) %}
-- do not need to re-query for tokens we already have so just add to complete table once
-- especially with the low rate limit
SELECT
    contract_address,
    DATE_PART('EPOCH', _inserted_timestamp) :: INTEGER AS partition_key,
    _inserted_timestamp,
    contract_address AS nearblocks_ft_complete_id,
    COALESCE(inserted_timestamp, _inserted_timestamp) AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    near.silver.ft_contract_metadata

{% else %}
SELECT
    VALUE :CONTRACT_ADDRESS :: STRING AS contract_address,
    partition_key,
    _inserted_timestamp,
    contract_address AS nearblocks_ft_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__nearblocks_ft_metadata') }}
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
{% endif %}

qualify(row_number() over (partition by contract_address order by _inserted_timestamp desc)) = 1
