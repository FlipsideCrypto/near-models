{{ config(
    materialized = 'view'
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
    insert_date AS _load_timestamp
FROM
    {{ source(
        'crosschain',
        'address_labels'
    ) }}
WHERE
    blockchain = 'near'
