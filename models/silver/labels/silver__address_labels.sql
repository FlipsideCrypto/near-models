{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'address',
    cluster_by = 'address',
    tags = ['scheduled_non_core'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address); DELETE FROM {{ this }} WHERE _is_deleted = TRUE;"
) }}

WITH address_labels AS (

    SELECT
        system_created_at,
        blockchain,
        address,
        creator,
        label_type,
        label_subtype,
        address_name,
        project_name,
        labels_combined_id,
        _is_deleted,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
        
    FROM
        {{ ref('bronze__address_labels') }}

    {% if is_incremental() %}
    WHERE _modified_timestamp >= (
        SELECT
            MAX(_modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
)
SELECT
    system_created_at,
    blockchain,
    address,
    creator,
    address_name,
    project_name,
    label_type,
    label_subtype,
    label_type AS l1_label,
    label_subtype AS l2_label,
    _inserted_timestamp,
    _is_deleted,
    labels_combined_id  as address_labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    address_labels
