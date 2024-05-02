{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'action_id_social',
    cluster_by = ['_inserted_timestamp::date', '_partition_by_block_number'],
    tags = ['curated', 'social']
) }}

WITH decoded_actions AS (

    SELECT
        tx_hash,
        action_id_social,
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
        node = 'post'

    {% if var("MANUAL_FIX") %}
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
        _inserted_timestamp,
        _modified_timestamp
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
    _inserted_timestamp,
    _modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id_social']
    ) }} AS social_posts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    posts
