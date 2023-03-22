{{ config(
    severity = 'error',
    enabled = False
) }}

WITH s3_index AS (

    SELECT
        *,
        SPLIT(
            NAME,
            '/'
        ) [3] :: NUMBER AS block_id,
        SPLIT(
            NAME,
            's3://stg-us-east-1-serverless-near-lake-mainnet-fsc/'
        ) [1] :: STRING AS _filename,
        FLOOR(
            block_id,
            -4
        ) AS _partition_by_block_number,
        SPLIT(SPLIT(_filename, '/') [1] :: STRING, '.json') [0] :: STRING AS TYPE
    FROM
        {{ target.database }}.bronze_api.s3_filenames
),
existing_shards AS (
    SELECT
        block_id,
        VALUE,
        _load_timestamp,
        _partition_by_block_number,
        _filename
    FROM
        {{ ref('silver__load_shards') }}
),
existing_blocks AS (
    SELECT
        block_id,
        VALUE,
        _load_timestamp,
        _partition_by_block_number,
        _filename
    FROM
        {{ ref('silver__load_blocks') }}
),
FINAL AS (
    SELECT
        s3._filename AS _filename_s3,
        COALESCE(
            sfs._filename,
            sfb._filename
        ) AS _filename_sf,
        SPLIT(
            TYPE,
            '_'
        ) [0] :: STRING AS filetype,
        s3.block_id,
        s3._partition_by_block_number
    FROM
        s3_index s3
        LEFT JOIN existing_shards sfs USING (_filename)
        LEFT JOIN existing_blocks sfb USING (_filename)
)
SELECT
    *
FROM
    FINAL
WHERE
    _filename_sf IS NULL
