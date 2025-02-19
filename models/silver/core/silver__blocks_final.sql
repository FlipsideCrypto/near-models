{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::date"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'block_id',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_id, block_hash);",
    tags = ['scheduled_core']
) }}

{% if var('NEAR_MIGRATE_ARCHIVE', False) %}

    SELECT
        block_id,
        block_timestamp,
        block_hash,
        prev_hash,
        block_author,
        chunks_json,
        header_json,
        _partition_by_block_number,
        streamline_blocks_id AS blocks_final_id,
        inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('silver__streamline_blocks') }}

{% else %}

WITH blocks AS (
    SELECT
        block_id,
        block_timestamp,
        block_hash,
        block_json :prev_hash :: STRING AS prev_hash,
        block_json :author :: STRING AS block_author,
        block_json :chunks :: ARRAY AS chunks_json,
        block_json :header :: OBJECT AS header_json,
        partition_key AS _partition_by_block_number
    FROM
        {{ ref('silver__blocks_v2') }}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS blocks_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    blocks
