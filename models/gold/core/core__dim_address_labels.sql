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
        COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__address_labels') }}
)
SELECT
    *
FROM
    flipside_labels
