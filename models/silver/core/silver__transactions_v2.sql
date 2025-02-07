-- depends_on: {{ ref('bronze__transactions') }}
-- depends_on: {{ ref('bronze__FR_transactions') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "tx_hash",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash)",
    tags = ['scheduled_core']
) }}

WITH bronze_transactions AS (

    SELECT
        VALUE :BLOCK_ID :: INT AS block_id,
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
            AND DATA IS NOT NULL
        {% else %}
            {{ ref('bronze__FR_transactions') }}
        WHERE
            DATA IS NOT NULL
        {% endif %}
    )
SELECT
    block_id,
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
