{{ config (
    materialized = 'view',
    secure = true
) }}

SELECT
    record_id,
    offset_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx_count,
    header,
    ingested_at AS _ingested_at,
    _inserted_timestamp
FROM
    {{ source(
        "chainwalkers",
        "near_blocks"
    ) }}
