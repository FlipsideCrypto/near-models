{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', '_load_timestamp::DATE'],
    unique_key = 'block_id',
    full_refresh = False,
    tags = ['load_s3']
) }}

WITH blocksjson AS (

    SELECT
        block_id,
        VALUE,
        _filename,
        _load_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('bronze__streamline_blocks') }}
        {{ partition_batch_load(500000) }}
)
SELECT
    *
FROM
    blocksjson