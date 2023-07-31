{{ config(
    materialized = 'incremental',
    unique_key = 'action_id_social',
    cluster_by = ['_inserted_timestamp::date', '_partition_by_block_number'],
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
            {{ incremental_load_filter('_inserted_timestamp') }}
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
        parsed_node_data :type :: STRING AS post_type,
        parsed_node_data :text :: STRING AS post_text,
        parsed_node_data :image :: STRING AS post_image,
        _partition_by_block_number,
        _load_timestamp,
        _inserted_timestamp
    FROM
        decoded_actions
    WHERE
        TRY_PARSE_JSON(
            node_data :main
        ) IS NOT NULL
)
SELECT
    tx_hash,
    action_id_social,
    block_id,
    block_timestamp,
    signer_id,
    post_type,
    post_text,
    post_image,
    _partition_by_block_number,
    _load_timestamp,
    _inserted_timestamp
FROM
    posts
