{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'social_widgets_id',
    cluster_by = ['_inserted_timestamp::date', '_partition_by_block_number'],
    tags = ['curated', 'social']
) }}

WITH decoded_actions AS (

    SELECT
        tx_hash,
        action_id_social,
        block_id,
        block_timestamp,
        predecessor_id,
        signer_id,
        node_data,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver_social__decoded_actions') }}
    WHERE
        node = 'widget'
    
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
widgets AS (
    SELECT
        tx_hash,
        action_id_social,
        block_id,
        block_timestamp,
        predecessor_id,
        signer_id,
        node_data,
        KEY :: STRING AS widget_name,
        TRY_PARSE_JSON(
            value
        ) AS source_data,
        CONCAT(
            'https://near.social/#/',
            signer_id,
            '/widget/',
            widget_name
        ) AS widget_url,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        decoded_actions,
    LATERAL FLATTEN(
    input => node_data
    )
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
    source_data AS _source_data,
    node_data AS _node_data,
    _partition_by_block_number,
    _inserted_timestamp,
    _modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id_social', 'widget_name']
    ) }} AS social_widgets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    widgets
