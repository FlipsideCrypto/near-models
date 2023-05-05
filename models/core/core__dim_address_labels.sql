{{ config(
    materialized = 'view',
    secure = true,
    tags = ['core']
) }}

SELECT
    system_created_at,
    blockchain,
    address,
    address_name,
    project_name,
    label_type,
    label_subtype,
    l1_label,
    l2_label
FROM
    {{ ref('silver__address_labels') }}
