{{ config (
    materialized = 'view',
    tags = ['load', 'load_blocks']
) }}

WITH external_blocks AS (

    SELECT
        metadata$filename AS _filename,
        SPLIT(
            _filename,
            '/'
        ) [0] :: NUMBER AS block_id,
        SYSDATE() AS _load_timestamp,
        VALUE,
        _partition_by_block_number
    FROM
        {{ source(
            "streamline",
            "blocks"
        ) }}
),
meta AS (

    SELECT
        job_created_time AS _inserted_timestamp,
        file_name AS _filename
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD('day', -2, SYSDATE()),
                table_name => '{{ source( 'streamline', 'blocks' ) }}')
            ) A
),
FINAL AS (
    SELECT
        e._filename,
        e.block_id,
        e._load_timestamp,
        e.value,
        e._partition_by_block_number,
        m._inserted_timestamp
    FROM
        external_blocks e
        LEFT JOIN meta m USING (
            _filename
        )
)
SELECT
    *
FROM
    FINAL
