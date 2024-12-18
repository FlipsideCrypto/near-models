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

{% if execute and var('MANUAL_FIX') %}

    {% set missing_shards %}
        SELECT
            min(_partition_by_block_number) as min_partition,
            max(_partition_by_block_number) as max_partition,
            min(block_id) as min_block_id,
            max(block_id) as max_block_id
        FROM near.tests.chunk_gaps
    {% endset %}

    {% set missing_shards_result = run_query(missing_shards) %}
    {% set min_partition = missing_shards_result[0][0] %}
    {% set max_partition = missing_shards_result[0][1] %}
    {% set min_block_id = missing_shards_result[0][2] %}
    {% set max_block_id = missing_shards_result[0][3] %}

{% endif %}

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
    {% if var('MANUAL_FIX') %}
        WHERE
            _partition_by_block_number BETWEEN {{ min_partition }} AND {{ max_partition }}
    {% else %}
        WHERE
            _partition_by_block_number >= (
                SELECT
                MAX(_partition_by_block_number) - (3000 * {{ var('STREAMLINE_LOAD_LOOKBACK_HOURS') }})
            FROM
                {{ this }}
        )
    {% endif %}
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
                    -{{ var('STREAMLINE_LOAD_LOOKBACK_HOURS') }},
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
        e.value :chunk :: VARIANT AS chunk,
        e.value :receipt_execution_outcomes :: VARIANT AS receipt_execution_outcomes,
        e.value :shard_id :: NUMBER AS shard_number,
        e.value :state_changes :: VARIANT AS state_changes,
        e._partition_by_block_number,
        m._inserted_timestamp
    FROM
        external_shards e
        LEFT JOIN meta m USING (_filename)

    {% if var('MANUAL_FIX') %}
        WHERE
            block_id BETWEEN {{ min_block_id }} AND {{ max_block_id }}
    {% else %}
        {% if is_incremental() %}
            WHERE
            _inserted_timestamp >= (
                SELECT
                    MAX(_inserted_timestamp)
                FROM
                    {{ this }}
            )
    {% endif %}
    {% endif %}
)
SELECT
    _filename,
    _shard_number,
    shard_id,
    chunk,
    receipt_execution_outcomes,
    shard_number,
    state_changes,
    _partition_by_block_number,
    {% if var('MANUAL_FIX') %}COALESCE(_inserted_timestamp, SYSDATE()) AS _inserted_timestamp,{% else %} _inserted_timestamp,{% endif %}
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
