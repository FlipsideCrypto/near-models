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
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS blocks_final_id,
    COALESCE(
        inserted_timestamp,
        _inserted_timestamp,
        _load_timestamp
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        _inserted_timestamp,
        _load_timestamp
    ) AS modified_timestamp,
    _invocation_id
FROM
    {{ ref('silver__streamline_blocks') }}

    {% if var("NEAR_MIGRATE_ARCHIVE") %}
      WHERE
        {{ partition_load_manual('no_buffer') }}
    {% endif %}
