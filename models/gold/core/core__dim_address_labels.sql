{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH flipside_labels AS (

    SELECT
        system_created_at,
        blockchain,
        address,
        address_name,
        project_name,
        label_type,
        label_subtype,
        l1_label,
        l2_label,
        creator,
        address_labels_id AS dim_address_labels_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__address_labels') }}
)
SELECT
    *
FROM
    flipside_labels
