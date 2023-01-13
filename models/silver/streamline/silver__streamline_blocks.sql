{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_partition_by_block_number', '_load_timestamp::DATE'],
    unique_key = 'block_id'
) }}

WITH blocksjson AS (

    SELECT
        *
    FROM
        {{ ref('silver__load_blocks') }}
        {# Note - no partition load as blocks is lightweight enough #}
    WHERE
        {{ incremental_load_filter('_load_timestamp') }}
        qualify ROW_NUMBER() over (
            PARTITION BY block_id
            ORDER BY
                _load_timestamp DESC
        ) = 1
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
        -- TODO tx_count not included in the data, may count manually and append in view
        [] AS events,
        -- events does not exist, Figment created this
        VALUE :chunks :: ARRAY AS chunks,
        VALUE :header :: OBJECT AS header,
        _partition_by_block_number,
        _load_timestamp
    FROM
        blocksjson
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
        {# the core view contains a number of columns that are found in the header #}
        header,
        _partition_by_block_number,
        _load_timestamp
    FROM
        blocks
)
SELECT
    *
FROM
    FINAL
