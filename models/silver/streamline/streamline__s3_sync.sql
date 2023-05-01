{{ config(
    materialized = 'view',
    tags = ['s3_copy'],
    post_hook = "select * from {{this.schema}}.{{this.identifier}}",
    comment = "incrementally sync Streamline.near_dev.blocks and Streamline.near_dev.shards"
) }}

WITH latest_block AS (

    SELECT
        COALESCE(MAX(SPLIT_PART(file_name, '/', 1)), (
    SELECT
        LPAD(MAX(block_id) :: STRING, 12, '0')
    FROM
        {{ ref("silver__load_blocks") }})) AS block_id
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                table_name => 'streamline.near_dev.blocks',
                start_time => DATEADD('hour', {{ var("S3_LOOKBACK_HOURS", -2) }}, SYSDATE()))
            )
            WHERE
                operation_status = 'REGISTERED_NEW'
        ),
        blocks AS (
            -- Look back 1000 blocks from most max block_id and count forward 4000
            SELECT
                ROW_NUMBER() over (
                    ORDER BY
                        SEQ4()
                ) AS series,
                b.block_id :: INTEGER - 1000 + series AS new_block_id,
                RIGHT(REPEAT('0', 12) || new_block_id :: STRING, 12) AS prefix
            FROM
                TABLE(GENERATOR(rowcount => 10000)),
                latest_block b
        ),
        list_files AS (
            SELECT
                streamline.udf_s3_list_objects(
                    's3://near-lake-data-mainnet/' || b.prefix || '/*'
                ) AS files
            FROM
                blocks b
        )
    SELECT
        streamline.udf_s3_copy_objects(
            files,
            's3://near-lake-data-mainnet/',
            's3://stg-us-east-1-serverless-near-lake-mainnet-fsc/'
        ) AS synced
    FROM
        list_files l
    WHERE
        ARRAY_SIZE(
            l.files
        ) > 0
