{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['_inserted_timestamp::DATE', '_partition_by_block_number'],
    unique_key = 'shard_id',
    tags = ['load', 'load_shards','scheduled_core'],
    full_refresh = False
) }}

WITH external_shards AS (

    SELECT
        metadata$filename AS _filename,
        VALUE,
        _partition_by_block_number
    FROM
        {{ source(
            "streamline",
            "shards"
        ) }}
    WHERE
        _partition_by_block_number = 122556000
        and value:header:height::NUMBER between 122556416 and 122556516
),
meta AS (
    SELECT
        job_created_time AS _inserted_timestamp,
        file_name AS _filename
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD(
                    'hour', 
                    -320,
                    SYSDATE()
                ),
                table_name => '{{ source( 'streamline', 'shards' ) }}'
            )
        ) A
),
shards AS (
    SELECT
        e._filename,
        SPLIT(
            e._filename,
            '/'
        ) [0] :: NUMBER AS block_id,
        RIGHT(SPLIT(e._filename, '.') [0], 1) :: NUMBER AS _shard_number,
        concat_ws(
            '-',
            block_id :: STRING,
            _shard_number :: STRING
        ) AS shard_id,
        e.value :chunk :: variant AS chunk,
        e.value :receipt_execution_outcomes :: variant AS receipt_execution_outcomes,
        e.value :shard_id :: NUMBER AS shard_number,
        e.value :state_changes :: variant AS state_changes,
        e._partition_by_block_number,
        m._inserted_timestamp
    FROM
        external_shards e
        LEFT JOIN meta m USING (_filename)

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
