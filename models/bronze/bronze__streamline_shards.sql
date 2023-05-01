{{ config (
    materialized = 'view'
) }}

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
