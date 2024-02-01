{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'block_id',
    tags = ['load', 'load_blocks']
) }}

WITH load_blocks AS (

    SELECT
        block_id,
        VALUE,
        _filename,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_blocks') }}
    WHERE
        TRUE 
        {{ incremental_load_filter('_inserted_timestamp') }}
),
blocks AS (
    SELECT
        VALUE :header :height :: NUMBER AS block_id,
        TO_TIMESTAMP_NTZ(
            VALUE :header :timestamp :: STRING
        ) AS block_timestamp,
        VALUE :header :hash :: STRING AS block_hash,
        VALUE :header :prev_hash :: STRING AS prev_hash,
        VALUE :author :: STRING AS block_author,
        VALUE :header :gas_price :: NUMBER AS gas_price,
        VALUE :header :total_supply :: NUMBER AS total_supply,
        VALUE :header :validator_proposals :: ARRAY AS validator_proposals,
        VALUE :header :validator_reward :: NUMBER AS validator_reward,
        VALUE :header :latest_protocol_version :: NUMBER AS latest_protocol_version,
        VALUE :header :epoch_id :: STRING AS epoch_id,
        VALUE :header :next_epoch_id :: STRING AS next_epoch_id,
        NULL AS tx_count,
        -- tx_count is legacy field, deprecate from core view
        [] AS events,
        -- events does not exist, Figment created this
        VALUE :chunks :: ARRAY AS chunks,
        VALUE :header :: OBJECT AS header,
        _partition_by_block_number,
        _load_timestamp,
        _inserted_timestamp
    FROM
        load_blocks
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        block_hash,
        prev_hash,
        block_author,
        gas_price,
        total_supply,
        validator_proposals,
        validator_reward,
        latest_protocol_version,
        epoch_id,
        next_epoch_id,
        tx_count,
        events,
        chunks,
        header,
        _partition_by_block_number,
        _load_timestamp,
        _inserted_timestamp
    FROM
        blocks
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS streamline_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL 
    qualify ROW_NUMBER() over (
        PARTITION BY block_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
