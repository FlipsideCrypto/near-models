{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE', '_partition_by_block_number'],
    unique_key = 'block_id',
    tags = ['load', 'load_blocks','scheduled_core'],
    full_refresh = False
) }}
-- depends on {{ ref('bronze__blocks') }}
-- depends on {{ ref('bronze__FR_blocks') }}
WITH
blocks AS (
    SELECT
        value :header :height :: NUMBER AS block_id,
        TO_TIMESTAMP_NTZ(
            value :header :timestamp :: STRING
        ) AS block_timestamp,
        value :header :hash :: STRING AS block_hash,
        value :header :prev_hash :: STRING AS prev_hash,
        value :author :: STRING AS block_author,
        value :header :gas_price :: NUMBER AS gas_price,
        value :header :total_supply :: NUMBER AS total_supply,
        value :header :validator_proposals :: ARRAY AS validator_proposals,
        value :header :validator_reward :: NUMBER AS validator_reward,
        value :header :latest_protocol_version :: NUMBER AS latest_protocol_version,
        value :header :epoch_id :: STRING AS epoch_id,
        value :header :next_epoch_id :: STRING AS next_epoch_id,
        NULL AS tx_count,
        -- tx_count is legacy field, deprecate from core view
        [] AS events,
        -- events does not exist, Figment created this
        value :chunks :: ARRAY AS chunks,
        value :header :: OBJECT AS header,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        
    {% if is_incremental() %}
    {{ ref('bronze__blocks') }}
        WHERE
            _inserted_timestamp >= (
                SELECT
                    MAX(_inserted_timestamp) _inserted_timestamp
                FROM
                    {{ this }}
            )
    {% else %}
        {{ ref('bronze__FR_blocks') }}
    {% endif %}

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
    blocks 
    qualify ROW_NUMBER() over (
        PARTITION BY block_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
