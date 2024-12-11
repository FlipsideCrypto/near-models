{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ['DBT_INTERNAL_DEST._partition_by_block_number >= (select min(_partition_by_block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['_inserted_timestamp::DATE', '_partition_by_block_number'],
    unique_key = 'shard_id',
    tags = ['load', 'load_shards','scheduled_core'],
    full_refresh = False
) }}
-- depends on {{ ref('bronze__shards') }}
-- depends on {{ ref('bronze__FR_shards') }}
WITH 
shards AS (
    SELECT
        file_name,
        SPLIT(
            file_name,
            '/'
        ) [0] :: NUMBER AS block_id,
        RIGHT(SPLIT(file_name, '.') [0], 1) :: NUMBER AS _shard_number,
        concat_ws(
            '-',
            block_id :: STRING,
            _shard_number :: STRING
        ) AS shard_id,
        value :chunk :: VARIANT AS chunk,
        value :receipt_execution_outcomes :: VARIANT AS receipt_execution_outcomes,
        value :shard_id :: NUMBER AS shard_number,
        value :state_changes :: VARIANT AS state_changes,
        _partition_by_block_number,
        _inserted_timestamp
    FROM

    {% if is_incremental() %}
    {{ ref('bronze__shards') }}
        WHERE
            _inserted_timestamp >= (
                SELECT
                    MAX(_inserted_timestamp) _inserted_timestamp
                FROM
                    {{ this }}
            )
    {% else %}
        {{ ref('bronze__FR_shards') }}
    {% endif %}

)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['shard_id']
    ) }} AS streamline_shards_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    shards 
    qualify ROW_NUMBER() over (
        PARTITION BY shard_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1