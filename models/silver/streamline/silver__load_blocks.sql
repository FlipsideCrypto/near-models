{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_partition_by_block_number', '_inserted_timestamp::DATE'],
    unique_key = 'block_id',
    full_refresh = False,
    tags = ['load', 'load_blocks']
) }}

WITH {% if var("MANUAL_FIX") %}
    missed_blocks AS (

        SELECT
            _partition_by_block_number,
            missing_block_id
        FROM
            {{ ref('_missed_blocks') }}
    ),
{% endif %}

last_day AS (
    SELECT
        *
    FROM
        {{ ref('bronze__streamline_blocks') }}
    WHERE
        _partition_by_block_number >= (
            SELECT
                MIN(_partition_by_block_number)
            FROM
                {{ this }}
            WHERE
                _inserted_timestamp >= CURRENT_TIMESTAMP - interval '24 hours'
        )
),
blocks_json AS (
    SELECT
        block_id,
        VALUE,
        _filename,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp

        {% if var("MANUAL_FIX") %}
    FROM
        {{ ref('bronze__streamline_blocks') }}
    WHERE
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
    {% else %}
    FROM
        last_day
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
    {% endif %}
)
SELECT
    *
FROM
    blocks_json
