{{ config (
    materialized = 'view'
) }}

SELECT
    metadata$filename AS _filename,
    SPLIT(
        _filename,
        '/'
    ) [0] :: NUMBER AS block_id,
    CURRENT_TIMESTAMP :: TIMESTAMP_NTZ as _load_timestamp,
    VALUE,
    _partition_by_block_number
FROM
    {{ source(
        "streamline_dev",
        "blocks"
    ) }}
