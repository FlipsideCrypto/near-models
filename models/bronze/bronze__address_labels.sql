{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

SELECT
    system_created_at,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    insert_date as _inserted_timestamp,
    modified_timestamp,
    _is_deleted,
    labels_combined_id
FROM
    {{ source(
        'crosschain_silver',
        'labels_combined'
    ) }}
WHERE
    blockchain = 'near'
