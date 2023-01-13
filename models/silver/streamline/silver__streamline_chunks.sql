{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'chunk_hash',
    cluster_by = ['_load_timestamp::date','height_created','height_included'],
    tags = ['s3']
) }}

WITH shards AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_shards') }}

        {% if target.name == 'dev' %}
            {{ partition_batch_load_dev(1000000) }}
        {% else %}
            {{ partition_batch_load(5000000) }}
        {% endif %}
        AND chunk != 'null'
        AND {{ incremental_load_filter('_load_timestamp') }}
),
FINAL AS (
    SELECT
        block_id,
        shard_id,
        _load_timestamp,
        _partition_by_block_number,
        chunk,
        chunk :header :height_created :: NUMBER AS height_created,
        chunk :header :height_included :: NUMBER AS height_included,
        chunk :author :: STRING AS author,
        chunk :header :chunk_hash :: STRING AS chunk_hash,
        chunk :header :: OBJECT AS header,
        chunk :receipts :: ARRAY AS receipts,
        chunk :transactions :: ARRAY AS chunk_transactions
    FROM
        shards
)
SELECT
    *
FROM
    FINAL
