{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_partition_by_block_number', '_inserted_timestamp::DATE'],
    unique_key = 'shard_id',
    full_refresh = False,
    tags = ['load', 'load_shards']
) }}

WITH {% if var("MANUAL_FIX") %}
    missed_shards AS (

        SELECT
            _partition_by_block_number,
            bblock_id as block_id
        FROM
            {{ target.database }}.tests.chunk_gaps
    ),
{% endif %}

local_range AS (
    SELECT
        *
    FROM
        {{ ref('bronze__streamline_shards') }}
    WHERE
        {{ partition_incremental_load(
            75000,
            20000,
            0
        ) }}
),
shards_json AS (
    SELECT
        block_id,
        concat_ws(
            '-',
            block_id :: STRING,
            _shard_number :: STRING
        ) AS shard_id,
        _shard_number,
        VALUE,
        _filename,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp

        {% if var("MANUAL_FIX") %}
    FROM
        {{ ref('bronze__streamline_shards') }}
    WHERE
        _partition_by_block_number IN (
            SELECT
                DISTINCT _partition_by_block_number
            FROM
                missed_shards
        )
        AND block_id IN (
            SELECT
                DISTINCT block_id
            FROM
                missed_shards
        )
    {% else %}
    FROM
        local_range
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
    {% endif %}
)
SELECT
    *
FROM
    shards_json
