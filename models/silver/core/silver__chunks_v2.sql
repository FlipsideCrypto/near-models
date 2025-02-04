-- depends_on: {{ ref('bronze__chunks') }}
-- depends_on: {{ ref('bronze__FR_chunks') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "chunk_hash",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(chunk_hash)",
    tags = ['scheduled_core']
) }}

WITH bronze_chunks AS (

    SELECT
        VALUE :BLOCK_NUMBER :: INT AS block_number,
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
            AND DATA IS NOT NULL
        {% else %}
            {{ ref('bronze__FR_chunks') }}
        WHERE
            DATA IS NOT NULL
        {% endif %}
    )
SELECT
    block_number,
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
