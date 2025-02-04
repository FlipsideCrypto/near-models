-- depends_on: {{ ref('bronze__blocks') }}
-- depends_on: {{ ref('bronze__FR_blocks') }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "block_hash",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['scheduled_core']
) }}

WITH bronze_blocks AS (
    SELECT 
        VALUE :BLOCK_NUMBER :: INT AS block_number,
        DATA :header :hash :: STRING AS block_hash,
        partition_key,
        DATA :: VARIANT AS block_json,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze__blocks') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND DATA IS NOT NULL
    {% else %}
    {{ ref('bronze__FR_blocks') }}
    WHERE DATA IS NOT NULL
    {% endif %}
)

SELECT 
    block_number,
    block_hash,
    partition_key,
    block_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_hash']) }} AS blocks_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_blocks

QUALIFY ROW_NUMBER() OVER (PARTITION BY block_hash ORDER BY _inserted_timestamp DESC) = 1
