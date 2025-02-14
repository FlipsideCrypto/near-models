-- depends_on: {{ ref('bronze__chunks') }}
-- depends_on: {{ ref('bronze__FR_chunks') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::date"],
    unique_key = "chunk_hash",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(chunk_hash)",
    tags = ['scheduled_core']
) }}

WITH bronze_chunks AS (

    SELECT
        VALUE :BLOCK_ID :: INT AS block_id,
        VALUE :BLOCK_TIMESTAMP :: timestamp_ntz AS block_timestamp,
        DATA :header :shard_id :: INT AS shard_id,
        DATA :header :chunk_hash :: STRING AS chunk_hash,
        partition_key,
        DATA :: variant AS chunk_json,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__chunks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1900-01-01' :: TIMESTAMP) AS _inserted_timestamp
        FROM
            {{ this }})
            AND typeof(DATA) != 'NULL_VALUE'
        {% else %}
            {{ ref('bronze__FR_chunks') }}
        WHERE
            typeof(DATA) != 'NULL_VALUE'
        {% endif %}
    )
SELECT
    block_id,
    block_timestamp,
    shard_id,
    chunk_hash,
    partition_key,
    chunk_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['chunk_hash']) }} AS chunks_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze_chunks 

qualify ROW_NUMBER() over (
        PARTITION BY chunk_hash
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
