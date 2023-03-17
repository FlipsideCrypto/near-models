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
        AND node = 'post'
),
posts AS (
    SELECT
        tx_hash,
        action_id_social,
        block_id,
        block_timestamp,
        signer_id,
        TRY_PARSE_JSON(
            node_data :main
        ) AS parsed_node_data,
        -- TODO consider these column names
        parsed_node_data :type :: STRING AS post_type,
        parsed_node_data :text :: STRING AS post_text,
        parsed_node_data :image :: STRING AS post_image
    FROM
        decoded_actions
    WHERE
        PARSE_JSON(
            node_data :main
        ) IS NOT NULL
)
SELECT
    posts
FROM
    abc
