{{ config(
    materialized = var('LIVE_TABLE_MATERIALIZATION', 'incremental'),
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::date"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'block_id',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_id, block_hash);",
    tags = ['scheduled_core', 'core_v2'],
    full_refresh = false
) }}

{% if var('NEAR_MIGRATE_ARCHIVE', False) %}
    {% if execute %}
        {% do log('Migrating blocks ' ~ var('RANGE_START') ~ ' to ' ~ var('RANGE_END'), info=True) %}
        {% do log('Invocation ID: ' ~ invocation_id, info=True) %}
    {% endif %}
    SELECT
        block_id,
        block_timestamp,
        block_hash,
        prev_hash,
        block_author,
        chunks_json,
        header_json,
        _partition_by_block_number,
        blocks_final_id,
        inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('_migrate_blocks') }}

{% else %}

WITH blocks AS (
    SELECT
        block_id,
        block_timestamp,
        block_hash,
        block_json :header :prev_hash :: STRING AS prev_hash,
        block_json :author :: STRING AS block_author,
        block_json :chunks :: ARRAY AS chunks_json,
        block_json :header :: OBJECT AS header_json,
        partition_key AS _partition_by_block_number
    FROM
        {{ ref('silver__blocks_v2') }}

        {% if is_incremental() %}
        WHERE
            modified_timestamp >= (
            SELECT
                COALESCE(MAX(modified_timestamp), '1970-01-01')
            FROM
                {{ this }}
            )
        {% endif %}
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

{% endif %}
