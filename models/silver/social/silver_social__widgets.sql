{{ config(
    materialized = 'incremental',
    unique_key = 'action_id_social',
    cluster_by = ['_load_timestamp::date', '_partition_by_block_number'],
    tags = ['curated', 'social']
) }}

WITH decoded_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver_social__decoded_actions') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
        AND node = 'widget'
),
widgets AS (
    SELECT
        tx_hash,
        action_id_social,
        block_id,
        block_timestamp,
        signer_id,
        node_data,
        object_keys(TRY_PARSE_JSON(node_data)) [0] :: STRING AS widget_name,
        try_parse_json(node_data [widget_name]) AS source_data,
        CONCAT(
            'https://near.social/#/',
            signer_id,
            '/widget/',
            widget_name
        ) AS widget_url,
        _partition_by_block_number,
        _load_timestamp
    FROM
        decoded_actions
)
SELECT
    tx_hash,
    action_id_social,
    block_id,
    block_timestamp,
    signer_id,
    widget_name,
    source_data :"" :: STRING AS source_code,
    TRY_PARSE_JSON(
        source_data :metadata
    ) AS metadata,
    TRY_PARSE_JSON(
        source_data :branch
    ) AS branch,
    TRY_PARSE_JSON(
        source_data :widgetModulesUsed
    ) AS widget_modules_used,
    widget_url,
    source_data as _source_data,
    node_data as _node_data,
    _partition_by_block_number,
    _load_timestamp
FROM
    widgets
