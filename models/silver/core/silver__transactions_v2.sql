-- depends_on: {{ ref('bronze__transactions') }}
-- depends_on: {{ ref('bronze__FR_transactions') }}
{{ config (
    materialized = var('LIVE_TABLE_MATERIALIZATION', 'incremental'),
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate","origin_block_timestamp::date"],
    unique_key = "tx_hash",
    cluster_by = ['modified_timestamp::DATE','origin_block_timestamp::date'],
    tags = ['scheduled_core', 'core_v2']
) }}

WITH bronze_transactions AS (

    SELECT
        VALUE :BLOCK_ID :: INT AS origin_block_id,
        VALUE :BLOCK_TIMESTAMP_EPOCH :: INT AS origin_block_timestamp_epoch,
        VALUE :SHARD_ID :: INT AS shard_id,
        VALUE :CHUNK_HASH :: STRING AS chunk_hash,
        VALUE :HEIGHT_CREATED :: INT AS chunk_height_created,
        VALUE :HEIGHT_INCLUDED :: INT AS chunk_height_included,
        DATA :transaction :hash :: STRING AS tx_hash,
        DATA :transaction :signer_id :: STRING AS signer_id,
        partition_key,
        DATA :: variant AS response_json,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1900-01-01' :: TIMESTAMP) AS _inserted_timestamp
        FROM
            {{ this }})
            AND typeof(DATA) != 'NULL_VALUE'
        {% else %}
            {{ ref('bronze__FR_transactions') }}
        WHERE
            typeof(DATA) != 'NULL_VALUE'
        {% endif %}
    )
SELECT
    origin_block_id,
    origin_block_timestamp_epoch,
    TO_TIMESTAMP_NTZ(origin_block_timestamp_epoch, 9) AS origin_block_timestamp,
    shard_id,
    chunk_hash,
    chunk_height_created,
    chunk_height_included,
    tx_hash,
    signer_id,
    partition_key,
    response_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash']) }} AS transactions_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze_transactions 

qualify ROW_NUMBER() over (
        PARTITION BY tx_hash
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
