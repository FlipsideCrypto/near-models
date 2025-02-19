{{ config(
    materialized = 'ephemeral'
) }}

SELECT
    block_id,
    block_timestamp,
    block_hash,
    prev_hash,
    block_author,
    chunks AS chunks_json,
    header AS header_json,
    _partition_by_block_number,
    streamline_blocks_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    {{ ref('silver__streamline_blocks') }}

    {% if var("BATCH_MIGRATE") %}
      WHERE
        {{ partition_load_manual('no_buffer') }}
    {% endif %}
