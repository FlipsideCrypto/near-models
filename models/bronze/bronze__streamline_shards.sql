{{ config (
    materialized = 'view'
) }}
with external_shards as (
SELECT
    metadata$filename AS _filename,
    SPLIT(
        _filename,
        '/'
    ) [0] :: NUMBER AS block_id,
    CURRENT_TIMESTAMP :: TIMESTAMP_NTZ AS _load_timestamp,
    RIGHT(SPLIT(_filename, '.') [0], 1) :: NUMBER AS _shard_number,
    VALUE,
    _partition_by_block_number
FROM
    {{ source(
        "streamline_dev",
        "shards"
    ) }}
),
meta AS (
    SELECT
        job_created_time AS _inserted_timestamp,
        file_name AS _filename,
        SPLIT(
            _filename,
            '/'
        ) [0] :: NUMBER AS block_id,
        FLOOR(
            block_id,
            -4
        ) AS _partition_by_block_number
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                table_name => '{{ source( 'streamline_dev', 'shards' ) }}')
            ) A
        ),
        FINAL AS (
            SELECT
                e._filename,
                e.block_id,
                IFF(
                    m._inserted_timestamp IS NULL,
                    e._load_timestamp,
                    NULL
                ) AS _load_timestamp,
                e._shard_number,
                e.value,
                e._partition_by_block_number,
                m._inserted_timestamp
            FROM
                external_shards e
                LEFT JOIN meta m USING (
                    block_id,
                    _partition_by_block_number
                )
        )
    SELECT
        *
    FROM
        FINAL