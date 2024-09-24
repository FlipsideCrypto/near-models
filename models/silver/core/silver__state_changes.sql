{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST._partition_by_block_number >= (select min(_partition_by_block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'state_change_id',
    cluster_by = ['modified_timestamp::date', '_partition_by_block_number']
) }}
-- https://app.snowflake.com/zsniary/exa10207/#/compute/history/queries/01b73a95-0411-29e0-3d4f-83027386d7bb/detail
-- Took 3h16m on 2XL
WITH shards AS (

    SELECT
        block_id,
        shard_id,
        state_changes,
        _partition_by_block_number,
        COALESCE(
            modified_timestamp,
            _inserted_timestamp
        ) AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_shards') }}
    WHERE
        ARRAY_SIZE(state_changes) > 0 

{% if var('MANUAL_FIX') %}
    AND {{ partition_load_manual('no_buffer') }}
{% else %}

    {% if is_incremental() %}
    AND _modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
{% endif %}
),
flatten_state_change AS (
    SELECT
        block_id,
        shard_id,
        VALUE,
        INDEX,
        VALUE :cause :: variant AS state_change_cause,
        VALUE :change :: variant AS state_changes,
        VALUE :type :: STRING AS state_change_type,
        _partition_by_block_number
    FROM
        shards,
        LATERAL FLATTEN (state_changes)
)
SELECT
    block_id,
    shard_id,
    value,
    index,
    state_change_cause,
    state_changes,
    state_change_type,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(['shard_id', 'index::STRING']) }} AS state_change_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_state_change
