-- depends_on: {{ ref('bronze__blocks') }}
-- depends_on: {{ ref('bronze__FR_blocks') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::date"],
    unique_key = "block_hash",
    cluster_by = ['modified_timestamp::DATE','block_timestamp::date'],
    tags = ['scheduled_core', 'core_v2']
) }}

WITH bronze_blocks AS (

    SELECT
        VALUE :BLOCK_ID :: INT AS block_id,
        DATA :header :hash :: STRING AS block_hash,
        DATA :header :timestamp :: INT AS block_timestamp_epoch,
        partition_key,
        DATA :: variant AS block_json,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1900-01-01' :: TIMESTAMP) AS _inserted_timestamp
        FROM
            {{ this }})
            AND typeof(DATA) != 'NULL_VALUE'
        {% else %}
            {{ ref('bronze__FR_blocks') }}
        WHERE
            typeof(DATA) != 'NULL_VALUE'
        {% endif %}
    )
SELECT
    block_id,
    block_hash,
    block_timestamp_epoch,
    TO_TIMESTAMP_NTZ(block_timestamp_epoch, 9) AS block_timestamp,
    partition_key,
    block_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_hash']) }} AS blocks_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze_blocks 

qualify ROW_NUMBER() over (
        PARTITION BY block_hash
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
