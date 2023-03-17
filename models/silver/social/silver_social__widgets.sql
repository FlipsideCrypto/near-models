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
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
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
        node_data [widget_name] AS js_code,
        CONCAT(
            'https://near.social/#/',
            signer_id,
            '/widget/',
            widget_name
        ) AS widget_url
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
    js_code,
    widget_url
FROM
    widgets
