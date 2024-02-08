{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'action_id_profile',
    cluster_by = ['block_timestamp::date', 'signer_id'],
    tags = ['curated', 'social']
) }}

WITH decoded_actions AS (

    SELECT
        action_id_social,
        tx_hash,
        block_id,
        block_timestamp,
        signer_id,
        node_data,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver_social__decoded_actions') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if var('IS_MIGRATION') %}
                {{ incremental_load_filter('_inserted_timestamp') }}
            {% else %}
                {{ incremental_load_filter('_modified_timestamp') }}
            {% endif %}
        {% endif %}
        AND node = 'profile'
),
flatten_profile_json AS (
    SELECT
        concat_ws(
            '-',
            action_id_social,
            key
        ) AS action_id_profile,
        action_id_social,
        tx_hash,
        block_id,
        block_timestamp,
        signer_id,
        key AS profile_section,
        VALUE :: STRING AS profile_data,
        -- must store as string due to various possible inputs
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        decoded_actions,
        LATERAL FLATTEN(node_data)
)
SELECT
    action_id_social,
    action_id_profile,
    tx_hash,
    block_id,
    block_timestamp,
    signer_id,
    profile_section,
    profile_data,
    _partition_by_block_number,
    _inserted_timestamp,
    _modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id_profile']
    ) }} AS social_profile_changes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_profile_json
