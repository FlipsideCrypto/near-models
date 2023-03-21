{{ config(
    materialized = 'incremental',
    unique_key = 'action_id_profile',
    cluster_by = ['block_timestamp::date', 'signer_id'],
    tags = ['curated', 'social']
) }}

WITH decoded_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver_social__decoded_actions') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
    AND
        node = 'profile'
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
        _load_timestamp,
        _partition_by_block_number
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
    _load_timestamp,
    _partition_by_block_number
FROM
    flatten_profile_json
