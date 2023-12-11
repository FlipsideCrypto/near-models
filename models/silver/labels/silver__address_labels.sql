{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'address',
    cluster_by = 'address',
    tags = ['labels']
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
        _load_timestamp,
        _inserted_timestamp
    FROM
        {{ ref('bronze__address_labels') }}

{% if is_incremental() %}
WHERE
    {{ incremental_load_filter('_inserted_timestamp') }}
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
    _load_timestamp,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS address_labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    address_labels
