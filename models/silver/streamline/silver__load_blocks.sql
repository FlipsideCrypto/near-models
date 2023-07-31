{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_partition_by_block_number', '_inserted_timestamp::DATE'],
    unique_key = 'block_id',
    full_refresh = False,
    tags = ['load', 'load_blocks']
) }}

WITH {% if var("MANUAL_FIX") %}
    missing_blocks AS (

        SELECT
            _partition_by_block_number,
            missing_block_id
        FROM
            {{ target.database }}.tests.streamline_block_gaps
    ),
{% endif %}

blocks_json AS (
    SELECT
        block_id,
        VALUE,
        _filename,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_blocks') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            _partition_by_block_number IN (
                SELECT
                    DISTINCT _partition_by_block_number
                FROM
                    missing_blocks
            )
            AND block_id IN (
                SELECT
                    missing_block_id
                FROM
                    missing_blocks
            )
        {% else %}
            WHERE
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
)
SELECT
    *
FROM
    blocks_json
