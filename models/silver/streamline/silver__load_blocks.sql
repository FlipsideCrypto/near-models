{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', '_load_timestamp::DATE'],
    unique_key = 'block_id',
    full_refresh = False,
    tags = ['load', 'load_blocks']
) }}

WITH {# missed_blocks AS (

SELECT
    missing_block_id,
    _partition_by_block_number
FROM
    {{ ref('_missed_blocks') }}
),
#}
blocks_json AS (
    SELECT
        block_id,
        VALUE,
        _filename,
        _load_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('bronze__streamline_blocks') }}
    WHERE
        {{ partition_batch_load(150000) }}
        -- lookback for late blocks
        {# OR (
        _partition_by_block_number IN (
            SELECT
                DISTINCT _partition_by_block_number
            FROM
                missed_blocks
        )
        AND block_id IN (
            SELECT
                missing_block_id
            FROM
                missed_blocks
        )
) #}
),
meta AS (
    SELECT
        registered_on AS _inserted_timestamp,
        file_name AS _filename
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => {{ source(
                    'streamline_dev',
                    'blocks'
                ) }}
            )
        )
    WHERE
        _filename IN (
            SELECT
                DISTINCT _filename
            FROM
                blocks_json
        )
)
SELECT
    block_id,
    VALUE,
    _filename,
    _load_timestamp,
    _partition_by_block_number,
    _inserted_timestamp
FROM
    blocks_json
    LEFT JOIN meta USING (_filename)
