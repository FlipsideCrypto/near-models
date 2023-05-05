{{ config(
    materialized = 'incremental',
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
        _load_timestamp
    FROM
        {{ ref('bronze__address_labels') }}

{% if is_incremental() %}
WHERE
    _load_timestamp >= (
        SELECT
            MAX(_load_timestamp)
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
    _load_timestamp
FROM
    address_labels
