{{ config(
    materialized = 'view',
    secure = true
) }}

SELECT
    system_created_at,
    blockchain,
    address,
    address_name,
    project_name,
    label_type,
    label_subtype,
    label_type AS l1_label,
    label_subtype AS l2_label
FROM
    {{ source(
        'crosschain',
        'address_labels'
    ) }}
WHERE
    blockchain = 'near'
